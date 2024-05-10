package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Limits     Limits            `yaml:"limits"`
	Namespaces []string          `yaml:"namespaces"`
	Clients    map[string]Client `yaml:"clients"`
}

func (c *Config) setDefaults() error {
	if err := c.Limits.setDefaults(); err != nil {
		return fmt.Errorf("error setting defaults for limits: %w", err)
	}

	return nil
}

type Limits struct {
	MaxBytesPerRecord           int `yaml:"max_bytes_per_record"`
	ExplorationObjectFieldLimit int `yaml:"exploration_object_field_limit"`
	ExplorationArrayEntryLimit  int `yaml:"exploration_array_entry_limit"`
	CapturedValueLengthLimit    int `yaml:"captured_value_length_limit"`
	TotalKeyLimit               int `yaml:"total_key_limit"`
}

func (l *Limits) setDefaults() error {
	if l.MaxBytesPerRecord == 0 {
		l.MaxBytesPerRecord = 1024 * 1024
	}
	if l.ExplorationObjectFieldLimit == 0 {
		l.ExplorationObjectFieldLimit = 1000
	}
	if l.ExplorationArrayEntryLimit == 0 {
		l.ExplorationArrayEntryLimit = 1000
	}
	if l.CapturedValueLengthLimit == 0 {
		l.CapturedValueLengthLimit = 1024
	}
	if l.TotalKeyLimit == 0 {
		l.TotalKeyLimit = 1000
	}

	return nil
}

type Client struct {
	SharedSecret string                 `yaml:"shared_secret"`
	Access       map[string]AccessLevel `yaml:"access"`
}

type AccessLevel struct {
	QueryAccess  bool `yaml:"query_access"`
	IngestAccess bool `yaml:"ingest_access"`
}

func (c Config) Validate() error {
	if len(c.Clients) == 0 {
		return errors.New("no clients configured")
	}

	if len(c.Namespaces) == 0 {
		return errors.New("no namespaces configured")
	}

	nsMap := map[string]bool{}
	for _, ns := range c.Namespaces {
		if nsMap[ns] {
			return fmt.Errorf("duplicate namespace %q", ns)
		}
		nsMap[ns] = true
	}

	for username, client := range c.Clients {
		if len(client.SharedSecret) < 64 {
			return errors.New("shared secret must be at least 64 characters")
		}

		for ns, access := range client.Access {
			if !nsMap[ns] {
				return fmt.Errorf("client %q has access to unknown namespace %q", username, ns)
			}

			if !access.QueryAccess && !access.IngestAccess {
				return fmt.Errorf("client %q has no access to namespace %q", username, ns)
			}
		}
	}
	return nil
}

func Load(filenameOrData string) (*Config, error) {
	var config Config

	var data []byte

	if strings.HasPrefix(filenameOrData, "{") {
		data = []byte(filenameOrData)
	} else {
		content, err := os.ReadFile(filenameOrData)
		if err != nil {
			return nil, err
		}
		data = content
	}

	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("error unmarshaling config: %w", err)
	}

	if err := config.setDefaults(); err != nil {
		return nil, fmt.Errorf("error setting defaults: %w", err)
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("validation error: %w", err)
	}

	return &config, nil
}
