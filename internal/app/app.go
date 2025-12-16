package app

import (
	"backend/internal/config"
	"backend/internal/http"
	"backend/internal/service/backend"
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

type App struct {
}

func NewApp() *App {
	return &App{}
}

func (*App) Run(cfg config.Config) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_1_0

	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	producer, err := sarama.NewSyncProducer([]string{cfg.Kafka.Addr}, config)
	if err != nil {
		log.Fatalf("failed to create Kafka sync producer: %s", err.Error())
	}
	service := backend.NewVideoProcessingService(cfg, producer, cfg.Cameras, cfg.HTTP.FramesDir)
	server := http.NewMediaServer(cfg.HTTP, service)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		server.Serve()
	}()
	defer func() {
		server.Stop(context.Background())
	}()

	<-stop
}
