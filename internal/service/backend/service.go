package backend

import (
	"backend/internal/config"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	_ "image/jpeg"
	_ "image/png"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/vitali-fedulov/images/v2"
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

	go func() {
		err := svc.consumeDetectionResults(context.Background())
		if err != nil {
			logger.Error("Failed to consume detection results", zap.Error(err))
		}
	}()

	return svc
}

// ProcessVideoFile extracts frames AND timestamps in one go
func (vps *VideoProcessingService) ProcessVideoFile(ctx context.Context, cameraID string, videoPath string, startTime time.Time) error {
	vps.logger.Infof("Processing video from %s, started at %s", cameraID, startTime)

	// 1. EXTRACT FRAMES AND TIMESTAMPS TOGETHER
	// This replaces both extractKeyframes and getKeyframeOffsets calls
	allFramePaths, allOffsets, err := vps.extractFramesAndTimestamps(cameraID, videoPath)
	if err != nil {
		return fmt.Errorf("extraction failed: %w", err)
	}

	countRaw := len(allFramePaths)
	vps.logger.Infof("Extracted %d frames and timestamps", countRaw)

	// 2. FILTER DUPLICATES (vitali-fedulov/images)
	finalPaths, finalOffsets, err := vps.filterDuplicateFrames(allFramePaths, allOffsets)
	if err != nil {
		vps.logger.Errorf("Filtering failed, sending all frames: %v", err)
		finalPaths = allFramePaths
		finalOffsets = allOffsets
	}

	// 3. SEND TASKS
	countFiltered := len(finalPaths)
	for i := 0; i < countFiltered; i++ {
		offsetDuration := time.Duration(finalOffsets[i] * float64(time.Second))
		frameTime := startTime.Add(offsetDuration)

		if err := vps.sendFrameTask(cameraID, finalPaths[i], frameTime); err != nil {
			vps.logger.Errorf("Failed to send frame task: %v", err)
		} else {
			vps.numberNotReadyFrames++
			vps.totalFrames++
		}
	}

	vps.logger.Infof("Camera %s: Extracted %d -> Filtered %d (Dropped %d duplicates)",
		cameraID, countRaw, countFiltered, countRaw-countFiltered)

	os.Remove(videoPath)
	return nil
}

// extractFramesAndTimestamps runs ffmpeg ONCE to save images and parse showinfo for timestamps
func (vps *VideoProcessingService) extractFramesAndTimestamps(cameraID, videoPath string) ([]string, []float64, error) {
	timestamp := time.Now().Unix()
	outputPattern := filepath.Join(vps.framesDir, fmt.Sprintf("camera_%s_%d_%%04d.jpg", cameraID, timestamp))

	// We add "showinfo" filter to print frame info to stderr
	cmd := exec.Command("ffmpeg",
		"-y",
		"-i", videoPath,
		"-vf", "select='eq(pict_type,I)',showinfo", // <--- ADDED showinfo
		"-vsync", "vfr",
		"-frame_pts", "true",
		"-q:v", "2",
		// We can't use -loglevel error because we need info output.
		// We grep/filter manually later.
		outputPattern,
	)

	// Capture Stderr (ffmpeg writes logs to stderr)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	vps.logger.Infof("FFmpeg command: %s", cmd.String())
	err := cmd.Run()
	if err != nil {
		return nil, nil, fmt.Errorf("ffmpeg error: %w, stderr: %s", err, stderr.String())
	}

	// 1. Find the files that were created
	pattern := filepath.Join(vps.framesDir, fmt.Sprintf("camera_%s_%d_*.jpg", cameraID, timestamp))
	framePaths, err := filepath.Glob(pattern)
	if err != nil {
		return nil, nil, fmt.Errorf("glob error: %w", err)
	}

	// 2. Parse Timestamps from Stderr output
	// Example line: [Parsed_showinfo_0 @ 0x...] n:   0 pts:    7200 pts_time:0.08    pos:      ...
	outputLog := stderr.String()
	offsets := []float64{}

	// Regex to find: pts_time:123.456
	re := regexp.MustCompile(`pts_time:([\d\.]+)`)

	scanner := bufio.NewScanner(strings.NewReader(outputLog))
	for scanner.Scan() {
		line := scanner.Text()
		// Only look at showinfo lines
		if strings.Contains(line, "Parsed_showinfo") || strings.Contains(line, "pts_time:") {
			matches := re.FindStringSubmatch(line)
			if len(matches) > 1 {
				ts, err := strconv.ParseFloat(matches[1], 64)
				if err == nil {
					offsets = append(offsets, ts)
				}
			}
		}
	}

	// Sanity Check
	if len(framePaths) != len(offsets) {
		vps.logger.Warnf("Warning: Mismatch in extraction! Files: %d, Timestamps: %d", len(framePaths), len(offsets))
		// Truncate to safe length
		minLen := len(framePaths)
		if len(offsets) < minLen {
			minLen = len(offsets)
		}
		framePaths = framePaths[:minLen]
		offsets = offsets[:minLen]
	}

	return framePaths, offsets, nil
}

// filterDuplicateFrames uses perceptual hash (v4)
func (vps *VideoProcessingService) filterDuplicateFrames(paths []string, offsets []float64) ([]string, []float64, error) {
	if len(paths) == 0 {
		return nil, nil, nil
	}

	uniquePaths := make([]string, 0, len(paths))
	uniqueOffsets := make([]float64, 0, len(offsets))

	uniquePaths = append(uniquePaths, paths[0])
	uniqueOffsets = append(uniqueOffsets, offsets[0])

	lastImg, err := images.Open(paths[0])
	if err != nil {
		return paths, offsets, fmt.Errorf("failed to open first image: %w", err)
	}
	lastHash, lastHashSz := images.Hash(lastImg)

	for i := 1; i < len(paths); i++ {
		currPath := paths[i]

		currImg, err := images.Open(currPath)
		if err != nil {
			vps.logger.Warnf("Skipping bad frame %s: %v", currPath, err)
			os.Remove(currPath)
			continue
		}

		currHash, currHashSZ := images.Hash(currImg)

		if images.Similar(lastHash, currHash, lastHashSz, currHashSZ) {
			// Duplicate
			os.Remove(currPath)
		} else {
			// Unique
			uniquePaths = append(uniquePaths, currPath)
			uniqueOffsets = append(uniqueOffsets, offsets[i])

			lastHash = currHash
			lastHashSz = currHashSZ
		}
	}

	return uniquePaths, uniqueOffsets, nil
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
