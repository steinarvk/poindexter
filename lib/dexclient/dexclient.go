package dexclient

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mitchellh/go-homedir"
	"github.com/steinarvk/poindexter/lib/dexerror"
	"github.com/steinarvk/poindexter/lib/logging"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

type Selector struct {
	Namespace   string
	Server      string
	User        string
	AccessGroup string
}

type Client struct {
	Scheme string `yaml:"scheme"`
	Host   string `yaml:"host"`
	Port   int    `yaml:"port"`

	User     string `yaml:"user"`
	Password string `yaml:"password"`

	Namespace string `yaml:"namespace"`
}

func DefaultConfigFilename(ctx context.Context) (string, error) {
	rv, err := homedir.Expand("~/.config/poindexter/poindexter-client.yaml")
	if err != nil {
		return "", err
	}
	return rv, nil
}

func ConfigFilenames(ctx context.Context) ([]string, error) {
	logger := logging.FromContext(ctx)

	var rv []string

	if env := os.Getenv("POINDEXTER_CLIENT_CONFIG"); env != "" {
		directories := strings.Split(env, ":")
		rv = append(rv, directories...)
	}

	defaultFilename, err := DefaultConfigFilename(ctx)
	if err != nil {
		logger.Warn("failed to determine default config filename", zap.Error(err))
	} else {
		rv = append(rv, defaultFilename)
	}

	return rv, nil
}

func ReadSecret(secret *SecretConfig) (string, error) {
	if secret == nil {
		return "", dexerror.New(
			dexerror.WithPublicMessage("no secret configuration provided"),
		)
	}

	if secret.Filename != "" {
		data, err := os.ReadFile(secret.Filename)
		if err != nil {
			return "", dexerror.New(
				dexerror.WithPublicMessage("failed to read secret file"),
				dexerror.WithPublicData("filename", secret.Filename),
				dexerror.WithPublicData("error", err.Error()),
			)
		}
		return strings.TrimSpace(string(data)), nil
	}

	if secret.EnvVar != "" {
		return os.Getenv(secret.EnvVar), nil
	}

	return "", nil
}

func LoadConfig(ctx context.Context) (*Config, error) {
	logger := logging.FromContext(ctx)

	filenames, err := ConfigFilenames(ctx)
	if err != nil {
		return nil, err
	}

	var configs []*ConfigFile

	for _, fn := range filenames {
		if _, err := os.Stat(fn); err != nil {
			if os.IsNotExist(err) {
				if len(configs) == 0 {
					logger.Warn("config file does not exist", zap.String("filename", fn))
				}
				continue
			}
			return nil, err
		}

		// Make absolute
		fn, _ := filepath.Abs(fn)

		data, err := os.ReadFile(fn)
		if err != nil {
			return nil, dexerror.New(
				dexerror.WithPublicMessage("failed to read config file"),
				dexerror.WithPublicData("filename", fn),
				dexerror.WithPublicData("error", err.Error()),
			)
		}

		var cfg ConfigFile
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return nil, dexerror.New(
				dexerror.WithPublicMessage("failed to parse config file"),
				dexerror.WithPublicData("filename", fn),
				dexerror.WithPublicData("error", err.Error()),
			)
		}

		cfg.Filename = fn
		configs = append(configs, &cfg)
	}

	return &Config{
		ConfigFiles: configs,
	}, nil
}

func (c *Config) ServerConfigs() []*ServerConfig {
	var rv []*ServerConfig
	for _, cf := range c.ConfigFiles {
		rv = append(rv, cf.Servers...)
	}
	return rv
}

func (s *ServerConfig) HasUser(username string) bool {
	for _, u := range s.Users {
		if u.Username == username {
			return true
		}
	}
	return false
}

func (s *ServerConfig) GetUserConfig(username string) *UserConfig {
	if username == "" {
		return nil
	}
	for _, u := range s.Users {
		if u.Username == username {
			return u
		}
	}
	return nil
}

func (s *ServerConfig) GetNamespaceConfig(namespace string) *NamespaceConfig {
	if namespace == "" {
		return nil
	}
	for _, ns := range s.Namespaces {
		if ns.Name == namespace {
			return ns
		}
	}
	return nil
}

func (s *ServerConfig) HasNamespace(namespace string) bool {
	return s.GetNamespaceConfig(namespace) != nil
}

func (s *ServerConfig) HasAlias(alias string) bool {
	for _, a := range s.Aliases {
		if a != alias {
			return true
		}
	}
	return false
}

func filterServers(xs []*ServerConfig, f func(*ServerConfig) bool) []*ServerConfig {
	var rv []*ServerConfig
	for _, x := range xs {
		if f(x) {
			rv = append(rv, x)
		}
	}
	return rv
}

var (
	errNoServer = dexerror.New(
		dexerror.WithPublicMessage("no server configuration found"),
	)

	errNoMatchingServer = dexerror.New(
		dexerror.WithPublicMessage("no matching server configuration found"),
	)

	errNoMatchingUser = dexerror.New(
		dexerror.WithPublicMessage("no matching user configuration found"),
	)
)

func New(ctx context.Context, cfg *Config, selector Selector) (*Client, error) {
	logger := logging.FromContext(ctx)

	// Namespace MUST be specified manually.
	if selector.Namespace == "" {
		return nil, fmt.Errorf("no namespace specified")
	}

	servers := cfg.ServerConfigs()

	if len(servers) == 0 {
		return nil, errNoServer
	}

	// If a server was specified, choose that server (by alias or host).

	if selector.Server != "" {
		servers = filterServers(servers, func(x *ServerConfig) bool {
			return x.HasAlias(selector.Server) || x.Host == selector.Server
		})
	}

	// If username is specified, filter down to servers that have this user.
	// Ending up empty here is fatal.

	if selector.User != "" {
		servers = filterServers(servers, func(x *ServerConfig) bool {
			return x.HasUser(selector.User)
		})
	}

	if len(servers) == 0 {
		return nil, errNoMatchingServer
	}

	// Try to filter down to servers that have the namespace configured.
	// Ending up empty here is OK, then abort filtering.

	if selector.Namespace != "" {
		maybeServers := filterServers(servers, func(x *ServerConfig) bool {
			return x.HasNamespace(selector.Namespace)
		})
		if len(maybeServers) > 0 {
			servers = maybeServers
		}
	}

	// Choose the first remaining server.
	serverCfg := servers[0]

	username := selector.User

	if username == "" {
		// If a user was not specified, look for a default user in the namespace config.
		// Choose the matching namespace configuration (within the server).
		maybeNamespaceCfg := serverCfg.GetNamespaceConfig(selector.Namespace)

		if maybeNamespaceCfg != nil {
			defaultUsers := maybeNamespaceCfg.DefaultUsers
			if defaultUsers != nil {
				if defaultUsers.Query == defaultUsers.Ingest {
					username = defaultUsers.Query
				} else if selector.AccessGroup == "query" {
					username = defaultUsers.Query
				} else if selector.AccessGroup == "ingest" {
					username = defaultUsers.Ingest
				}
			}
		}
	}

	// Otherwise, if the user is still ambiguous, error.
	userCfg := serverCfg.GetUserConfig(username)
	if userCfg == nil {
		return nil, errNoMatchingUser
	}

	password, err := ReadSecret(userCfg.Password)
	if err != nil {
		return nil, err
	}

	rv := &Client{
		Scheme:    serverCfg.Scheme,
		Host:      serverCfg.Host,
		Port:      serverCfg.Port,
		User:      userCfg.Username,
		Password:  password,
		Namespace: selector.Namespace,
	}

	if rv.Scheme == "" {
		rv.Scheme = "https"
	}

	logger.Debug(
		"chose client configuration",
		zap.String("namespace", selector.Namespace),
		zap.String("selector_server", selector.Server),
		zap.String("selector_user", selector.User),
		zap.String("chosen_host", rv.Host),
		zap.String("chosen_scheme", rv.Scheme),
		zap.String("chosen_user", rv.User),
	)

	return rv, nil
}
