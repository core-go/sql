package sql

type DatabaseConfig struct {
	MultiStatements bool   `mapstructure:"multiStatements"`
	Dialect         string `mapstructure:"dialect"`
	Host            string `mapstructure:"host"`
	Port            int    `mapstructure:"port"`
	Database        string `mapstructure:"database"`
	User            string `mapstructure:"user"`
	Password        string `mapstructure:"password"`
}
