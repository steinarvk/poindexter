package version

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"sync"
)

type Info struct {
	CommitHash  string
	CommitTime  string
	DirtyCommit bool
	BinaryHash  string
}

func (v Info) VersionString() string {
	var rv string
	if v.CommitHash != "" {
		if v.DirtyCommit {
			rv = fmt.Sprintf("%s-dirty", v.CommitHash[:16])
		} else {
			rv = v.CommitHash[:16]
		}
	}

	if (rv == "" || v.DirtyCommit) && v.BinaryHash != "" {
		if rv != "" {
			rv += "@"
		}
		rv += fmt.Sprintf("sha256:%s", v.BinaryHash[:8])
	}

	return rv
}

var (
	globalVersion    *Info
	globalVersionErr error
	globalOnce       sync.Once

	ForceHash bool = false
)

func GetInfo() (*Info, error) {
	onceBody := func() {
		globalVersion, globalVersionErr = computeVersionInfo(ForceHash)
	}
	globalOnce.Do(onceBody)
	return globalVersion, globalVersionErr
}

func computeVersionInfo(forceHash bool) (*Info, error) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return nil, errors.New("failed to read build info")
	}

	var rv Info

	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" {
			rv.CommitHash = setting.Value
		}
		if setting.Key == "vcs.modified" {
			rv.DirtyCommit = setting.Value == "true"
		}
		if setting.Key == "vcs.time" {
			rv.CommitTime = setting.Value
		}
	}

	if rv.CommitHash == "" || rv.DirtyCommit || forceHash {
		execPath, err := os.Executable()
		if err != nil {
			return nil, err
		}

		file, err := os.Open(execPath)
		if err != nil {
			return nil, err
		}
		defer file.Close()

		h := sha256.New()

		if _, err := io.Copy(h, file); err != nil {
			return nil, err
		}

		hexdigest := fmt.Sprintf("%x", h.Sum(nil))
		rv.BinaryHash = hexdigest
	}

	return &rv, nil
}
