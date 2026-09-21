package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"

	"github.com/kunchenguid/no-mistakes/internal/shellenv"
)

// cursorAgent is the subscription-only native Cursor CLI transport. Cursor's
// ACP server currently advertises only base model IDs and a medium reasoning
// variant, while the native CLI advertises parameterized IDs such as
// gpt-5.6-luna-xhigh. Keeping this transport explicit preserves the requested
// model identity instead of translating it to an ACP model that would change
// the effective reasoning level.
type cursorAgent struct {
	bin    string
	model  string
	effort string
	subprocessContext
}

func (a *cursorAgent) Name() string { return "cursor" }

func (a *cursorAgent) ReportsAgentAttempts() bool { return true }

func (a *cursorAgent) Run(ctx context.Context, opts RunOpts) (*Result, error) {
	return runWithRetry(ctx, "cursor", opts, claudeMaxRetries, classifyTransient, nil, func() (*Result, error) {
		// Re-run the real subscription check inside the retry closure so a
		// quota change cannot be bypassed by a transient process retry.
		if subscriptionOnly() {
			if err := runSubscriptionCheck(ctx, "cursor", a.model, a.effort); err != nil {
				return nil, err
			}
		}
		return a.runOnce(ctx, opts)
	})
}

func (a *cursorAgent) Close() error { return nil }

func (a *cursorAgent) runOnce(ctx context.Context, opts RunOpts) (*Result, error) {
	prompt := opts.Prompt
	if len(opts.JSONSchema) > 0 {
		prompt = buildACPStructuredPrompt(prompt, opts.JSONSchema)
	}
	cmd := exec.CommandContext(ctx, a.bin, a.buildArgs()...)
	cmd.Dir = opts.CWD
	cmd.Stdin = strings.NewReader(prompt)
	cmd.Env = a.gitSafeEnv(opts.CWD, opts.Env)
	shellenv.ConfigureShellCommand(cmd)

	var stderrBuf []byte
	var stderrWG sync.WaitGroup
	started, err := startNativeAgentCommand(cmd, nativeAgentActivityObserver(opts, "cursor"))
	if err != nil {
		return nil, fmt.Errorf("cursor start: %w", err)
	}
	defer started.closePipes()
	pid := started.pid()
	emitAgentStarted(opts, "cursor", pid)

	stderrWG.Add(1)
	go func() {
		defer stderrWG.Done()
		stderrBuf, _ = io.ReadAll(started.stderr)
	}()

	parser := &cursorStreamParser{onChunk: opts.OnChunk}
	if err := parser.parse(ctx, started.stdout); err != nil {
		err = started.waitAfterParseError(err)
		stderrWG.Wait()
		retErr := fmt.Errorf("cursor parse events: %w", err)
		emitAgentExited(opts, "cursor", pid, retErr)
		return parser.failedResult(), retErr
	}

	waitErr := started.wait()
	stderrWG.Wait()
	if waitErr != nil {
		detail := strings.TrimSpace(string(stderrBuf))
		if detail != "" {
			if cursorUsageRefusal(detail) {
				waitErr = fmt.Errorf("preflight available but execution quota refused: %s", detail)
			} else {
				waitErr = fmt.Errorf("%w: %s", waitErr, detail)
			}
		}
		retErr := fmt.Errorf("cursor exited: %w", waitErr)
		emitAgentExited(opts, "cursor", pid, retErr)
		return parser.failedResult(), retErr
	}
	if parser.errorMessage != "" {
		retErr := fmt.Errorf("cursor reported error: %s", parser.errorMessage)
		emitAgentExited(opts, "cursor", pid, retErr)
		return parser.failedResult(), retErr
	}
	if parser.malformedResult {
		retErr := errors.New("cursor returned a malformed result event")
		emitAgentExited(opts, "cursor", pid, retErr)
		return parser.failedResult(), retErr
	}
	if !parser.terminal {
		retErr := errors.New("cursor returned no terminal result event")
		emitAgentExited(opts, "cursor", pid, retErr)
		return parser.failedResult(), retErr
	}
	if parser.model == "" {
		retErr := errors.New("cursor returned no effective model identity")
		emitAgentExited(opts, "cursor", pid, retErr)
		return parser.failedResult(), retErr
	}
	if !cursorModelMatches(a.model, parser.model) {
		retErr := fmt.Errorf("cursor served model %q, requested %q", parser.model, a.model)
		emitAgentExited(opts, "cursor", pid, retErr)
		return parser.failedResult(), retErr
	}

	text := parser.finalText()
	res, err := finalizeTextResult("cursor", text, opts.JSONSchema, parser.usage)
	if res != nil {
		res.SessionID = parser.sessionID
		res.Model = parser.model
		res.ModelProvider = "cursor"
	}
	emitAgentExited(opts, "cursor", pid, err)
	return res, err
}

func cursorUsageRefusal(detail string) bool {
	detail = strings.ToLower(detail)
	return strings.Contains(detail, "usage limit") ||
		strings.Contains(detail, "included usage exhausted") ||
		strings.Contains(detail, "on-demand usage")
}

// buildArgs uses Cursor's documented headless transport. The model is passed
// byte-for-byte so parameterized model IDs remain effective and auditable.
func (a *cursorAgent) buildArgs() []string {
	return []string{
		"--print",
		"--output-format", "stream-json",
		"--force",
		"--trust",
		"--model", a.model,
	}
}

type cursorStreamParser struct {
	onChunk         func(string)
	text            strings.Builder
	resultText      string
	model           string
	sessionID       string
	errorMessage    string
	malformedResult bool
	terminal        bool
	usage           TokenUsage
}

type cursorStreamEvent struct {
	Type      string          `json:"type"`
	Subtype   string          `json:"subtype"`
	SessionID string          `json:"session_id"`
	Model     string          `json:"model"`
	Result    json.RawMessage `json:"result"`
	IsError   json.RawMessage `json:"is_error"`
	Error     json.RawMessage `json:"error"`
	Message   *cursorMessage  `json:"message"`
	Usage     *cursorUsage    `json:"usage"`
}

type cursorMessage struct {
	Content []cursorContent `json:"content"`
}

type cursorContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type cursorUsage struct {
	InputTokens      *int `json:"inputTokens"`
	OutputTokens     *int `json:"outputTokens"`
	CacheReadTokens  *int `json:"cacheReadTokens"`
	CacheWriteTokens *int `json:"cacheWriteTokens"`
}

func (p *cursorStreamParser) parse(ctx context.Context, r io.Reader) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 256*1024*1024)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var event cursorStreamEvent
		if err := json.Unmarshal(line, &event); err != nil {
			var typeErr *json.UnmarshalTypeError
			if !errors.As(err, &typeErr) {
				// Cursor's stream-json contract is JSONL. Ignore unrelated
				// diagnostics so a warning cannot become assistant output.
				continue
			}
			// Keep the event type and raw payloads so an object-shaped
			// provider error cannot disappear as an ignored line.
		}
		if event.SessionID != "" {
			p.sessionID = event.SessionID
		}
		if event.Model != "" {
			p.model = event.Model
		}
		if event.Usage != nil {
			applyCursorUsage(&p.usage, event.Usage)
		}
		switch event.Type {
		case "assistant":
			if event.Message == nil {
				continue
			}
			for _, content := range event.Message.Content {
				if content.Type != "text" || content.Text == "" {
					continue
				}
				p.text.WriteString(content.Text)
				if p.onChunk != nil {
					p.onChunk(content.Text)
				}
			}
		case "result":
			p.terminal = true
			if len(event.Result) == 0 || bytes.Equal(bytes.TrimSpace(event.Result), []byte("null")) {
				p.malformedResult = true
			} else {
				var resultText string
				if err := json.Unmarshal(event.Result, &resultText); err != nil {
					p.malformedResult = true
				} else {
					p.resultText = resultText
				}
			}
			if cursorEventIsError(event.IsError) || (event.Subtype != "" && event.Subtype != "success") {
				p.errorMessage = cursorPayloadText(event.Error)
				if p.errorMessage == "" {
					p.errorMessage = cursorPayloadText(event.Result)
				}
				if p.errorMessage == "" {
					p.errorMessage = "cursor result reported failure"
				}
			}
		case "error":
			p.errorMessage = cursorPayloadText(event.Error)
			if p.errorMessage == "" {
				p.errorMessage = cursorPayloadText(event.Result)
			}
			if p.errorMessage == "" {
				p.errorMessage = "cursor emitted an error event"
			}
		}
	}
	return scanner.Err()
}

func applyCursorUsage(dst *TokenUsage, src *cursorUsage) {
	if src.InputTokens != nil {
		dst.InputTokens = *src.InputTokens
		dst.Reported = true
	}
	if src.OutputTokens != nil {
		dst.OutputTokens = *src.OutputTokens
		dst.Reported = true
	}
	if src.CacheReadTokens != nil {
		dst.CacheReadTokens = *src.CacheReadTokens
		dst.Reported = true
	}
	if src.CacheWriteTokens != nil {
		dst.CacheCreationTokens = *src.CacheWriteTokens
		dst.CacheCreationReported = true
		dst.Reported = true
	}
}

func (p *cursorStreamParser) finalText() string {
	if p.resultText != "" {
		return p.resultText
	}
	return p.text.String()
}

func cursorPayloadText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text
	}
	var compact bytes.Buffer
	if json.Compact(&compact, raw) == nil {
		return compact.String()
	}
	return "provider error"
}

func cursorEventIsError(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) || bytes.Equal(raw, []byte("false")) {
		return false
	}
	var value bool
	if json.Unmarshal(raw, &value) == nil {
		return value
	}
	// A provider error object or string in is_error is malformed according to
	// Cursor's schema, but it is still a failure signal and must not be hidden.
	return true
}

func cursorModelMatches(requested, served string) bool {
	requestedIdentity := parseCursorModelIdentity(requested)
	servedIdentity := parseCursorModelIdentity(served)
	return requestedIdentity.valid && servedIdentity.valid &&
		requestedIdentity.base == servedIdentity.base &&
		requestedIdentity.effort == servedIdentity.effort &&
		requestedIdentity.fast == servedIdentity.fast
}

type cursorModelIdentity struct {
	base   string
	effort string
	fast   bool
	valid  bool
}

// parseCursorModelIdentity accepts both native IDs (gpt-5.6-luna-xhigh-fast)
// and the display identity Cursor reports in stream-json (GPT-5.6 Luna 1M
// Extra High Fast). Bracketed ACP-style metadata is parsed when present so a
// contradictory reasoning or fast setting fails closed instead of being
// discarded during comparison.
func parseCursorModelIdentity(raw string) cursorModelIdentity {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return cursorModelIdentity{}
	}
	var bracket string
	if start := strings.IndexByte(raw, '['); start >= 0 {
		end := strings.LastIndexByte(raw, ']')
		if end < start {
			return cursorModelIdentity{}
		}
		bracket = raw[start+1 : end]
		raw = strings.TrimSpace(raw[:start])
	}
	fields := strings.FieldsFunc(raw, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9')
	})
	if len(fields) == 0 {
		return cursorModelIdentity{}
	}

	effort := ""
	fast := false
	base := make([]string, 0, len(fields))
	for i := 0; i < len(fields); i++ {
		field := fields[i]
		switch field {
		case "1m":
			continue
		case "fast":
			fast = true
			continue
		case "extra":
			if i+1 < len(fields) && fields[i+1] == "high" {
				effort = "xhigh"
				i++
				continue
			}
		case "none", "low", "medium", "high", "xhigh", "max":
			effort = field
			continue
		}
		base = append(base, field)
	}
	if len(base) == 0 {
		return cursorModelIdentity{}
	}

	bracketEffort, bracketFast, bracketValid := cursorBracketSettings(bracket)
	if !bracketValid || (bracketEffort != "" && effort != "" && bracketEffort != effort) ||
		(bracketFast != nil && *bracketFast != fast && strings.Contains(raw, "fast")) {
		return cursorModelIdentity{}
	}
	if bracketEffort != "" {
		if effort != "" && effort != bracketEffort {
			return cursorModelIdentity{}
		}
		effort = bracketEffort
	}
	if bracketFast != nil {
		if *bracketFast && !fast {
			fast = true
		} else if !*bracketFast && fast {
			return cursorModelIdentity{}
		}
	}
	if effort == "" {
		// Cursor's model IDs omit the word "medium" for the medium/default
		// variant, and its display name likewise omits it.
		effort = "medium"
	}
	return cursorModelIdentity{base: strings.Join(base, "-"), effort: effort, fast: fast, valid: true}
}

func cursorBracketSettings(raw string) (effort string, fast *bool, valid bool) {
	if strings.TrimSpace(raw) == "" {
		return "", nil, true
	}
	valid = true
	for _, part := range strings.Split(raw, ",") {
		key, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		switch key {
		case "reasoning", "effort":
			canonical := strings.ReplaceAll(value, "_", "-")
			canonical = strings.ReplaceAll(canonical, " ", "-")
			switch canonical {
			case "none", "low", "medium", "high", "max":
				effort = canonical
			case "xhigh", "extra-high":
				effort = "xhigh"
			default:
				return "", nil, false
			}
		case "fast":
			switch value {
			case "true":
				v := true
				fast = &v
			case "false":
				v := false
				fast = &v
			default:
				return "", nil, false
			}
		}
	}
	return effort, fast, valid
}

func (p *cursorStreamParser) failedResult() *Result {
	res := failedResult(p.usage, p.sessionID)
	if res != nil {
		res.Model = p.model
		res.ModelProvider = "cursor"
	}
	return res
}
