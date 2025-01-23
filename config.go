package depoq

import (
	"fmt"
	"log"
	"net/url"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the entire YAML configuration
type Config struct {
	Version     string       `yaml:"version"`
	DataSources []DataSource `yaml:"datasources"`
	Runner      runner       `yaml:"runner"`
}

// DataSource represents a single data source configuration
type DataSource struct {
	Name   string        `yaml:"name"`
	Type   string        `yaml:"type"`
	Config ConfigDetails `yaml:"config"`
}

type runner struct {
	Paths []string `yaml:"paths"`
}

func (c *Config) FindByName(name string) (*DataSource, error) {
	for _, ds := range c.DataSources {
		if ds.Name == name {
			return &ds, nil
		}
	}
	return nil, fmt.Errorf("data source %s not found", name)
}

// ConfigDetails contains the details of the configuration
type ConfigDetails struct {
	Host            string     `yaml:"host"`
	Port            int        `yaml:"port"`
	Username        string     `yaml:"username"`
	Password        string     `yaml:"password"`
	DatabaseName    string     `yaml:"database_name"`
	Parameters      url.Values `yaml:"parameters"`
	ConnMaxIdleTime int        `yaml:"conn_max_idle_time"`
	ConnMaxLifetime int        `yaml:"conn_max_lifetime"`
	MaxOpenConns    int        `yaml:"max_open_conns"`
	MaxIdleConns    int        `yaml:"max_idle_conns"`
}

func loadConfig(filePath string) (*Config, error) {
	// Read the file
	yamlData, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read the file: %w", err)
	}

	var config Config

	// Unmarshal the YAML data into the Config struct
	err = yaml.Unmarshal([]byte(yamlData), &config)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	return &config, nil
}
