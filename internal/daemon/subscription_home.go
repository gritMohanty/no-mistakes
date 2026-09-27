package daemon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type managedSubscriptionPin struct {
	Enabled    bool
	Home       string
	PolicyFile string
	Check      string
}

type subscriptionPolicyFile struct {
	Registry string `json:"registry"`
}

type subscriptionRegistryFile struct {
	Firstmate struct {
		Home string `json:"home"`
	} `json:"firstmate"`
}

func resolveManagedSubscriptionPin() (managedSubscriptionPin, error) {
	if os.Getenv("GRIT_SUBSCRIPTION_POLICY") != "1" {
		return managedSubscriptionPin{}, nil
	}
	homeInput := os.Getenv("FM_HOME")
	policyInput := os.Getenv("GRIT_SUBSCRIPTION_POLICY_FILE")
	checkInput := os.Getenv("GRIT_SUBSCRIPTION_CHECK")
	if homeInput == "" {
		return managedSubscriptionPin{
			Enabled:    true,
			PolicyFile: filepath.Clean(policyInput),
			Check:      filepath.Clean(checkInput),
		}, nil
	}
	if !filepath.IsAbs(homeInput) || !filepath.IsAbs(policyInput) || !filepath.IsAbs(checkInput) {
		return managedSubscriptionPin{}, fmt.Errorf("Firstmate-managed subscription daemon requires absolute FM_HOME, GRIT_SUBSCRIPTION_POLICY_FILE, and GRIT_SUBSCRIPTION_CHECK")
	}
	home, err := canonicalPhysicalPath(homeInput)
	if err != nil {
		return managedSubscriptionPin{}, fmt.Errorf("resolve managed Firstmate home: %w", err)
	}
	policyFile, err := canonicalPhysicalPath(policyInput)
	if err != nil {
		return managedSubscriptionPin{}, fmt.Errorf("resolve managed subscription policy: %w", err)
	}
	check, err := canonicalPhysicalPath(checkInput)
	if err != nil {
		return managedSubscriptionPin{}, fmt.Errorf("resolve managed subscription check: %w", err)
	}
	var policy subscriptionPolicyFile
	if err := readRegularJSONNoSymlink(policyFile, &policy); err != nil {
		return managedSubscriptionPin{}, fmt.Errorf("read managed subscription policy: %w", err)
	}
	if !filepath.IsAbs(policy.Registry) {
		return managedSubscriptionPin{}, fmt.Errorf("managed subscription registry path is not absolute")
	}
	registryPath, err := canonicalPhysicalPath(policy.Registry)
	if err != nil {
		return managedSubscriptionPin{}, fmt.Errorf("resolve managed subscription registry: %w", err)
	}
	var registry subscriptionRegistryFile
	if err := readRegularJSONNoSymlink(registryPath, &registry); err != nil {
		return managedSubscriptionPin{}, fmt.Errorf("read managed subscription registry: %w", err)
	}
	registryHome, err := canonicalPhysicalPath(registry.Firstmate.Home)
	if err != nil {
		return managedSubscriptionPin{}, fmt.Errorf("resolve registry Firstmate home: %w", err)
	}
	if registryHome != home {
		return managedSubscriptionPin{}, fmt.Errorf("FM_HOME does not match firstmate.home in the managed Captain registry")
	}
	return managedSubscriptionPin{Enabled: true, Home: home, PolicyFile: policyFile, Check: check}, nil
}

func readRegularJSONNoSymlink(path string, target any) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	volume := filepath.VolumeName(abs)
	current := volume + string(filepath.Separator)
	relative := strings.TrimPrefix(strings.TrimPrefix(abs, volume), string(filepath.Separator))
	parts := strings.Split(relative, string(filepath.Separator))
	for index, part := range parts {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("path contains a symlink")
		}
		if index < len(parts)-1 && !info.IsDir() {
			return fmt.Errorf("parent path is not a directory")
		}
		if index == len(parts)-1 && !info.Mode().IsRegular() {
			return fmt.Errorf("file is not regular")
		}
	}
	before, err := os.Lstat(abs)
	if err != nil {
		return err
	}
	file, err := os.Open(abs)
	if err != nil {
		return err
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil {
		return err
	}
	if !after.Mode().IsRegular() || !os.SameFile(before, after) {
		return fmt.Errorf("file changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil {
		return err
	}
	if len(data) > 1<<20 {
		return fmt.Errorf("file exceeds size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("file contains trailing data")
	}
	return nil
}
