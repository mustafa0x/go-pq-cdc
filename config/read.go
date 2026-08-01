package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/go-playground/errors"
	"gopkg.in/yaml.v2"
)

func ReadConfigYAML(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, errors.Wrap(err, "read yaml config")
	}

	var cfg Config
	if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
		return Config{}, errors.Wrap(err, "yaml config file parse")
	}
	return cfg, nil
}

func ReadConfigJSON(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, errors.Wrap(err, "read json config")
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var cfg Config
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, errors.Wrap(err, "json config file parse")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return Config{}, fmt.Errorf("json config file parse: multiple JSON values")
		}
		return Config{}, errors.Wrap(err, "json config file parse")
	}
	return cfg, nil
}
