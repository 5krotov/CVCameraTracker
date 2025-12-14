package backend

import (
	"agent-service/internal/config"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type FrameTask struct {
	CameraID  string `json:"camera_id"`
	FramePath string `json:"frame_path"`
	Timestamp string `json:"timestamp"`
}

type ObjectDetectionResult struct {
	CameraID    string    `json:"camera_id"`
	FramePath   string    `json:"frame_path"`
	Timestamp   string    `json:"timestamp"`
	Coordinates []float64 `json:"coordinates"`
}

type DetectionResult struct {
	CameraID    string      `json:"camera_id"`
	FramePath   string      `json:"frame_path"`
	Timestamp   string      `json:"timestamp"`
	Objects     []string    `json:"objects"`
	Coordinates [][]float64 `json:"coordinates"`
}

type VideoProcessingService struct {
	config               config.Config
	kafka                sarama.SyncProducer
	logger               *zap.SugaredLogger
	cameras              []config.Camera
	framesDir            string
	numberNotReadyFrames int
	totalFrames          int

	mu               sync.Mutex
	detectionResults []DetectionResult
	resultPath       map[string][]ObjectDetectionResult
}

func NewVideoProcessingService(
	config config.Config,
	kafka sarama.SyncProducer,
	cameras []config.Camera,
	framesDir string,
) *VideoProcessingService {
	logger, _ := zap.NewProduction()

	if err := os.MkdirAll(framesDir, 0755); err != nil {
		logger.Fatal("Failed to create frames directory", zap.Error(err))
	}

	svc := &VideoProcessingService{
		config:    config,
		kafka:     kafka,
		logger:    logger.Sugar(),
		cameras:   cameras,
		framesDir: framesDir,
	}

	go svc.consumeDetectionResults(context.Background())

	return svc
}

func (vps *VideoProcessingService) ProcessVideoFile(ctx context.Context, cameraID string, videoPath string, startTime time.Time) error {
	vps.logger.Infof("Processing video from %s, started at %s", cameraID, startTime)

	framePaths, err := vps.extractKeyframes(cameraID, videoPath)
	if err != nil {
		return fmt.Errorf("extraction failed: %w", err)
	}

	offsets, err := vps.getKeyframeOffsets(videoPath)
	if err != nil {
		return fmt.Errorf("failed to get keyframe offsets: %w", err)
	}

	count := len(framePaths)
	if len(offsets) < count {
		count = len(offsets)
		vps.logger.Warnf("Mismatch: frames extracted (%d) > offsets found (%d). Truncating.", len(framePaths), len(offsets))
	}

	for i := 0; i < count; i++ {
		offsetDuration := time.Duration(offsets[i] * float64(time.Second))

		frameTime := startTime.Add(offsetDuration)

		if err := vps.sendFrameTask(cameraID, framePaths[i], frameTime); err != nil {
			vps.logger.Errorf("Failed to send frame task: %v", err)
		} else {
			vps.numberNotReadyFrames++
			vps.totalFrames++
		}
	}

	vps.logger.Infof("Processed %d frames for camera %s", count, cameraID)

	os.Remove(videoPath)

	return nil
}

func (vps *VideoProcessingService) getKeyframeOffsets(videoPath string) ([]float64, error) {
	cmd := exec.Command("ffprobe",
		"-v", "error",
		"-select_streams", "v:0",
		"-show_entries", "frame=pkt_pts_time",
		"-of", "csv=p=0",
		"-f", "lavfi",
		fmt.Sprintf("movie=%s,select=eq(pict_type\\,I)", videoPath),
	)

	var out bytes.Buffer
	cmd.Stdout = &out

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("ffprobe error: %w", err)
	}

	var offsets []float64
	scanner := bufio.NewScanner(&out)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		ts, err := strconv.ParseFloat(line, 64)
		if err != nil {
			continue
		}
		offsets = append(offsets, ts)
	}

	return offsets, nil
}

func (vps *VideoProcessingService) extractKeyframes(cameraID, videoPath string) ([]string, error) {
	timestamp := time.Now().Unix()
	outputPattern := filepath.Join(vps.framesDir, fmt.Sprintf("camera_%s_%d_%%04d.jpg", cameraID, timestamp))

	cmd := exec.Command("ffmpeg",
		"-i", videoPath,
		"-vf", "select='eq(pict_type,I)'",
		"-vsync", "vfr",
		"-frame_pts", "true",
		outputPattern,
	)

	vps.logger.Infof("Executing FFmpeg command: %s", cmd.String())

	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("ffmpeg error: %w, output: %s", err, string(output))
	}

	framePaths := []string{}
	pattern := filepath.Join(vps.framesDir, fmt.Sprintf("camera_%s_%d_*.jpg", cameraID, timestamp))

	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to find extracted frames: %w", err)
	}

	framePaths = append(framePaths, matches...)
	return framePaths, nil
}

func (vps *VideoProcessingService) sendFrameTask(cameraID, framePath string, captureTime time.Time) error {
	task := FrameTask{
		CameraID:  cameraID,
		FramePath: framePath,
		Timestamp: captureTime.Format(time.RFC3339Nano),
	}

	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal frame task: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: vps.config.Kafka.FrameTaskTopic,
		Key:   sarama.StringEncoder(cameraID),
		Value: sarama.ByteEncoder(data),
	}

	partition, _, err := vps.kafka.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send frame task to Kafka: %w", err)
	}

	vps.logger.Debugf("Frame task sent: cam=%s, ts=%s, part=%d",
		cameraID, task.Timestamp, partition)

	return nil
}
