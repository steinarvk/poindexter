package dexclient

type SecretConfig struct {
	Filename string `yaml:"filename"`
	EnvVar   string `yaml:"env_var"`
}

type UserConfig struct {
	Username string        `yaml:"username"`
	Password *SecretConfig `yaml:"password"`
}

type DefaultUsersConfig struct {
	Query  string `yaml:"query"`
	Ingest string `yaml:"ingest"`
}

type NamespaceConfig struct {
	Name         string              `yaml:"name"`
	DefaultUsers *DefaultUsersConfig `yaml:"default_users"`
}

type ServerConfig struct {
	Scheme      string             `yaml:"scheme"`
	Host        string             `yaml:"host"`
	Port        int                `yaml:"port"`
	Aliases     []string           `yaml:"aliases"`
	Users       []*UserConfig      `yaml:"users"`
	Namespaces  []*NamespaceConfig `yaml:"namespaces"`
	DefaultUser string             `yaml:"default_user"`
}

type ConfigFile struct {
	Filename string          `yaml:"filename"`
	Servers  []*ServerConfig `yaml:"servers"`
}

type Config struct {
	ConfigFiles []*ConfigFile `yaml:"config_files"`
}
