package orm

type DatabaseConfig struct {
	MultiStatements bool   `mapstructure:"multiStatements"`
	Provider        string `mapstructure:"provider"`
	Host            string `mapstructure:"host"`
	Port            int    `mapstructure:"port"`
	Database        string `mapstructure:"database"`
	User            string `mapstructure:"user"`
	Password        string `mapstructure:"password"`
}
