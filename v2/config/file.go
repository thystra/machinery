package config

import (
	"fmt"
	"github.com/RichardKnop/machinery/v2/log"
	"gopkg.in/yaml.v2"
	"os"
	"time"
)

func NewFromYaml(path string, keepReloading bool) (*Config, error) {
	cnf, err := fromFile(path)
	if err != nil {
		return nil, err
	}
	log.INFO.Printf("Successfully loaded config from file %s", path)
	if keepReloading {
		go func() {
			for {
				time.Sleep(reloadDelay)
				next, nextErr := fromFile(path)
				if nextErr != nil {
					log.WARNING.Printf("Failed to reload config from file %s: %v", path, nextErr)
					continue
				}
				*cnf = *next
			}
		}()
	}
	return cnf, nil
}

func ReadFromFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}
	return data, nil
}

func fromFile(path string) (*Config, error) {
	cnf := new(Config)
	*cnf = *defaultCnf
	if defaultCnf.Redis != nil {
		redisCnf := *defaultCnf.Redis
		cnf.Redis = &redisCnf
	}
	data, err := ReadFromFile(path)
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(data, cnf); err != nil {
		return nil, fmt.Errorf("unmarshal YAML: %w", err)
	}
	return cnf, nil
}
