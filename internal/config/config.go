package config

import (
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Kafka   KafkaConfig `yaml:"kafka"`
	Cameras []Camera    `yaml:"cameras"`
	HTTP    HTTPConfig  `yaml:"http"`
}

// HTTPConfig конфигурация для медиа-сервера
type HTTPConfig struct {
	Addr      string `yaml:"addr"`
	UseTLS    bool   `yaml:"use_tls"`
	CertFile  string `yaml:"cert_file"`
	KeyFile   string `yaml:"key_file"`
	UploadDir string `yaml:"upload_dir"`
	FramesDir string `yaml:"frames_dir"`
}

type Camera struct {
	ID        string  `yaml:"id"`
	Latitude  float64 `yaml:"latitude"`
	Longitude float64 `yaml:"longitude"`
}

type KafkaConfig struct {
	Addr                 string `yaml:"addr"`
	FrameTaskTopic       string `yaml:"frame_task_topic"`
	DetectionResultTopic string `yaml:"detection_result_topic"`
	DetectionResultGroup string `yaml:"detection_result_group"`
}

func NewConfig() *Config {
	return &Config{}
}

func (c *Config) Load(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	if err := yaml.Unmarshal(data, c); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}
	return nil
}
