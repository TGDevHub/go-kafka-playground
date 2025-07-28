package pool

import (
	"time"
)

type Config struct {
	Host                 string        `yaml:"host"`
	Port                 int           `yaml:"port"`
	User                 string        `yaml:"user"`
	Password             string        `yaml:"password"`
	Name                 string        `yaml:"name"`
	Timeout              time.Duration `yaml:"timeout"`
	TotalConnectionLimit int           `yaml:"total_connection_limit"`
	Collation            string        `yaml:"collation"`
}
