package main

import (
	"agent-service/internal/app"
	"agent-service/internal/config"
	"flag"
	"log"
)

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "/etc/agent-service/config.yaml", "path to config file")
	flag.Parse()

	cfg := config.NewConfig()
	err := cfg.Load(cfgPath)
	if err != nil {
		log.Fatalf("Error load config: %v", err)
		return
	}

	a := app.NewApp()
	a.Run(*cfg)
}
