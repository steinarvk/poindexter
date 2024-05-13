package syncdir

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v2"
)

type ClientConfig struct {
	Scheme string `yaml:"scheme"`
	Host   string `yaml:"host"`
	Port   int    `yaml:"port"`

	User     string `yaml:"user"`
	Password string `yaml:"password"`

	Namespace string `yaml:"namespace"`
}

type DirectoryConfig struct {
	RootDirectory  string         `yaml:"root_directory"`
	BaseNameRegexp *regexp.Regexp `yaml:"pattern"`
	ClientConfig   ClientConfig   `yaml:"client"`
}

var (
	defaultPattern = regexp.MustCompile(".*[.](jsonl|jsonlines)$")
)

func ReadConfig(configFilename string) (*DirectoryConfig, error) {
	var cfg DirectoryConfig

	info, err := os.Stat(configFilename)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		configFilename = filepath.Join(configFilename, "poindexter.yaml")
	}

	f, err := os.Open(configFilename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	if yaml.Unmarshal(data, &cfg) != nil {
		return nil, err
	}

	if cfg.RootDirectory == "" {
		cfg.RootDirectory = "."
	}

	if cfg.BaseNameRegexp == nil {
		cfg.BaseNameRegexp = defaultPattern
	}

	if cfg.ClientConfig.Scheme == "" {
		cfg.ClientConfig.Scheme = "https"
	}

	if cfg.ClientConfig.Host == "" {
		return nil, fmt.Errorf("client.host not configured")
	}

	if cfg.ClientConfig.User == "" {
		return nil, fmt.Errorf("client.user not configured")
	}

	if cfg.ClientConfig.Namespace == "" {
		return nil, fmt.Errorf("client.namespace not configured")
	}

	dir := cfg.RootDirectory

	// If directory is not absolute, interpret it relative to the directory of the config file
	if !strings.HasPrefix(dir, "/") {
		configDir := filepath.Dir(configFilename)
		dir = filepath.Join(configDir, dir)
		cfg.RootDirectory = dir
	}

	return &cfg, nil
}
