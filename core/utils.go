package core

import (
	"os"

	"gopkg.in/yaml.v2"
)

func ParseYaml(path string, value interface{}) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(content, value)
}
