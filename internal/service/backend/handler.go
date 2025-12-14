package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type DetectionResultHandler struct {
	logger *zap.SugaredLogger
	svc    *VideoProcessingService
}

func (h *DetectionResultHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *DetectionResultHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *DetectionResultHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var result DetectionResult
		if err := json.Unmarshal(msg.Value, &result); err != nil {
			h.logger.Warn("Failed to unmarshal detection result", zap.Error(err))
			continue
		}

		h.logger.Infof("Received detection result: camera=%s, frame=%s, objects=%v, coords=%v",
			result.CameraID, result.FramePath, result.Objects, result.Coordinates)

		if err := h.svc.saveDetectionResult(context.Background(), result); err != nil {
			h.logger.Errorf("Failed to save detection result: %v", err)
		}

		session.MarkMessage(msg, "")

	}
	return nil
}

func (vps *VideoProcessingService) consumeDetectionResults(ctx context.Context) error {
	vps.logger.Info("Starting Kafka consumer for detection results")

	config := sarama.NewConfig()
	config.Version = sarama.V2_8_1_0
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	groupID := vps.config.Kafka.DetectionResultGroup
	brokers := []string{vps.config.Kafka.Addr}
	topic := vps.config.Kafka.DetectionResultTopic

	handler := &DetectionResultHandler{logger: vps.logger, svc: vps}
	group, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		vps.logger.Fatal(err)
	}
	defer group.Close()

	for {
		if err := group.Consume(ctx, []string{topic}, handler); err != nil {
			vps.logger.Errorf(err.Error())
			time.Sleep(time.Second)
		}
	}
}

func (vps *VideoProcessingService) saveDetectionResult(ctx context.Context, result DetectionResult) error {
	vps.logger.Infof("Saving detection result for camera %s", result.CameraID)

	if err := os.Remove(result.FramePath); err != nil {
		vps.logger.Warnf("Failed to remove processed frame %s: %v", result.FramePath, err)
	}

	vps.mu.Lock()
	defer vps.mu.Unlock()

	vps.numberNotReadyFrames--
	vps.totalFrames++

	vps.detectionResults = append(vps.detectionResults, result)
	return nil
}

func (vps *VideoProcessingService) GetDetectionResult() (map[string][]ObjectDetectionResult, error) {
	if vps.numberNotReadyFrames != 0 {
		return nil, fmt.Errorf("VideoProcessing service is not ready: ready percentage %d %%", 100*(vps.numberNotReadyFrames/vps.totalFrames))
	}

	vps.mu.Lock()
	defer vps.mu.Unlock()

	if len(vps.detectionResults) != 0 {
		return vps.resultPath, nil
	}
	sort.Slice(vps.detectionResults, func(i, j int) bool {
		return vps.detectionResults[i].Timestamp < vps.detectionResults[j].Timestamp
	})

	vps.resultPath = make(map[string][]ObjectDetectionResult)
	for _, result := range vps.detectionResults {
		for i, obj := range result.Objects {
			objResult, ok := vps.resultPath[obj]
			if !ok {
				objResult = make([]ObjectDetectionResult, 0)
				objResult = append(objResult, ObjectDetectionResult{
					CameraID:    result.CameraID,
					Timestamp:   result.Timestamp,
					FramePath:   result.FramePath,
					Coordinates: result.Coordinates[i],
				})
				vps.resultPath[obj] = objResult

				continue
			}

			lastResult := objResult[len(objResult)-1]
			if lastResult.CameraID != result.CameraID {
				objResult = append(objResult, ObjectDetectionResult{
					CameraID:    result.CameraID,
					Timestamp:   result.Timestamp,
					FramePath:   result.FramePath,
					Coordinates: result.Coordinates[i],
				})

				vps.resultPath[obj] = objResult

				continue
			}
		}
	}
	return vps.resultPath, nil
}
