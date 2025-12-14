package http

import (
	"agent-service/internal/config"
	"agent-service/internal/service/backend"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
)

type MediaServer struct {
	config       config.HTTPConfig
	server       *http.Server
	videoService *backend.VideoProcessingService
	logger       *zap.SugaredLogger
}

func NewMediaServer(cfg config.HTTPConfig, videoService *backend.VideoProcessingService) *MediaServer {
	logger, _ := zap.NewProduction()

	mux := http.NewServeMux()
	srv := &MediaServer{
		config:       cfg,
		videoService: videoService,
		logger:       logger.Sugar(),
	}

	mux.HandleFunc("/api/v1/upload/video", srv.handleUploadVideo)
	mux.HandleFunc("/api/v1/results", srv.handleGetResults)

	srv.server = &http.Server{
		Addr:    cfg.Addr,
		Handler: mux,
	}

	return srv
}

func (s *MediaServer) Serve() error {
	s.logger.Infof("starting http media server on %v", s.config.Addr)

	if s.config.UseTLS {
		return s.server.ListenAndServeTLS(s.config.CertFile, s.config.KeyFile)
	}

	return s.server.ListenAndServe()
}

func (s *MediaServer) Stop(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *MediaServer) handleUploadVideo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	const maxUploadSize = 1000 << 20
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadSize)

	if err := r.ParseMultipartForm(maxUploadSize); err != nil {
		s.logger.Error("File too big or parse error", zap.Error(err))
		http.Error(w, "File too big", http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("video")
	if err != nil {
		http.Error(w, "Invalid file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	cameraID := r.FormValue("camera_id")
	if cameraID == "" {
		http.Error(w, "camera_id is required", http.StatusBadRequest)
		return
	}

	timestampStr := r.FormValue("timestamp")
	var startTime time.Time
	if timestampStr == "" {
		s.logger.Warn("Timestamp not provided, using current server time")
		startTime = time.Now()
	} else {
		parsedTime, err := time.Parse(time.RFC3339, timestampStr)
		if err != nil {
			http.Error(w, "Invalid timestamp format. Use RFC3339 (e.g. 2023-10-05T14:30:00Z)", http.StatusBadRequest)
			return
		}
		startTime = parsedTime
	}

	uploadDir := s.config.UploadDir
	if uploadDir == "" {
		uploadDir = "/tmp/videos_upload"
	}

	filename := fmt.Sprintf("upload_%s_%d%s", cameraID, startTime.Unix(), filepath.Ext(header.Filename))
	savePath := filepath.Join(uploadDir, filename)

	if err := os.MkdirAll(filepath.Dir(savePath), 0755); err != nil {
		s.logger.Error("Failed to create upload dir", zap.Error(err))
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	dst, err := os.Create(savePath)
	if err != nil {
		s.logger.Error("Unable to create file", zap.Error(err))
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	defer dst.Close()

	if _, err := io.Copy(dst, file); err != nil {
		s.logger.Error("Unable to copy file content", zap.Error(err))
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	s.logger.Infof("Video uploaded: %s (camera: %s, time: %s)", savePath, cameraID, startTime)

	go func() {
		ctx := context.Background()
		if err := s.videoService.ProcessVideoFile(ctx, cameraID, savePath, startTime); err != nil {
			s.logger.Error("Failed to process uploaded video", zap.Error(err))
		}
	}()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":    "uploaded",
		"file":      filename,
		"timestamp": startTime.Format(time.RFC3339),
	})
}

func (s *MediaServer) handleGetResults(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	results, err := s.videoService.GetDetectionResult()

	if err != nil {
		s.logger.Infof("Client requested results, but processing not finished: %v", err)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)

		json.NewEncoder(w).Encode(map[string]string{
			"status":  "processing",
			"message": err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(results); err != nil {
		s.logger.Error("Failed to encode results response", zap.Error(err))
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}
