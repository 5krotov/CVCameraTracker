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
	vps.logger.Debugf("GetDetectionResult called. Status: NotReady=%d, Total=%d", vps.numberNotReadyFrames, vps.totalFrames)

	if vps.numberNotReadyFrames != 0 {
		percentage := 0
		if vps.totalFrames > 0 {
			percentage = 100 * (vps.totalFrames - vps.numberNotReadyFrames) / vps.totalFrames
		}
		vps.logger.Warnf("Service not ready. Pending frames: %d/%d (%d%% complete)",
			vps.numberNotReadyFrames, vps.totalFrames, percentage)

		return nil, fmt.Errorf("VideoProcessing service is not ready: ready percentage %d %%", percentage)
	}

	vps.mu.Lock()
	defer vps.mu.Unlock()

	vps.logger.Debugf("Acquired lock. Existing resultPath len: %d, detectionResults len: %d", len(vps.resultPath), len(vps.detectionResults))

	// If resultPath is already populated, return it (caching behavior?)
	// Note: If you want to allow re-generation, remove this check or add a flag.
	if len(vps.resultPath) != 0 {
		vps.logger.Debug("Returning cached resultPath")
		return vps.resultPath, nil
	}

	if len(vps.detectionResults) == 0 {
		vps.logger.Warn("No detection results available to process")
		return nil, nil // Or error?
	}

	vps.logger.Debug("Sorting detection results by timestamp...")
	sort.Slice(vps.detectionResults, func(i, j int) bool {
		return vps.detectionResults[i].Timestamp < vps.detectionResults[j].Timestamp
	})
	vps.logger.Debugf("Sorted %d results. First: %s, Last: %s",
		len(vps.detectionResults),
		vps.detectionResults[0].Timestamp,
		vps.detectionResults[len(vps.detectionResults)-1].Timestamp)

	vps.resultPath = make(map[string][]ObjectDetectionResult)

	for idx, result := range vps.detectionResults {
		vps.logger.Debugf("Processing result %d/%d: Cam=%s, TS=%s, Objects=%v", idx+1, len(vps.detectionResults), result.CameraID, result.Timestamp, result.Objects)

		for i, obj := range result.Objects {
			// Check if we have coordinates for this object
			if i >= len(result.Coordinates) {
				vps.logger.Errorf("CRITICAL: Missing coordinates for object '%s' at index %d in frame %s", obj, i, result.FramePath)
				continue
			}

			coords := result.Coordinates[i]
			vps.logger.Debugf("  -> Object: %s, Coords: %v", obj, coords)

			objResult, ok := vps.resultPath[obj]
			if !ok {
				vps.logger.Debugf("     New object category detected: '%s'. initializing list.", obj)
				objResult = make([]ObjectDetectionResult, 0)

				newEntry := ObjectDetectionResult{
					CameraID:    result.CameraID,
					Timestamp:   result.Timestamp,
					FramePath:   result.FramePath,
					Coordinates: coords,
				}
				objResult = append(objResult, newEntry)
				vps.resultPath[obj] = objResult
				continue
			}

			// Logic: Add only if CameraID changes?
			// (This logic seems weird if it's the SAME camera but different time.
			//  Usually you want to track the object over time on the same camera too.
			//  But keeping your original logic intact with logs:)

			lastResult := objResult[len(objResult)-1]
			vps.logger.Debugf("     Existing list for '%s' has %d entries. Last CameraID: %s", obj, len(objResult), lastResult.CameraID)

			if lastResult.CameraID != result.CameraID {
				vps.logger.Debugf("     CameraID changed (%s -> %s). Appending new entry.", lastResult.CameraID, result.CameraID)

				objResult = append(objResult, ObjectDetectionResult{
					CameraID:    result.CameraID,
					Timestamp:   result.Timestamp,
					FramePath:   result.FramePath,
					Coordinates: coords,
				})

				vps.resultPath[obj] = objResult
			} else {
				vps.logger.Debugf("     Skipping append (CameraID %s match). Current logic filters consecutive frames from same camera?", result.CameraID)
				// DEBUG: If you meant to append ALL frames, your logic 'if lastResult.CameraID != ...' prevents tracking path!
				// Assuming this is intended for some specific "camera switch" logic.
			}
		}
	}

	vps.logger.Infof("Finished processing. Result keys found: %d", len(vps.resultPath))
	for k, v := range vps.resultPath {
		vps.logger.Debugf("  Key '%s': %d entries", k, len(v))
	}

	return vps.resultPath, nil
}
