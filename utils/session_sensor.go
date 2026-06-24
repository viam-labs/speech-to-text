package utils

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"sync"
	"time"

	"go.viam.com/rdk/app"
	"go.viam.com/rdk/components/sensor"
	"go.viam.com/rdk/data"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
)

// SessionSensor queues one reading per completed STT session and exposes them
// via Readings().
// On each push, the sensor uploads the session's WAV bytes to Viam's binary
// store via BinaryDataCaptureUpload and includes the returned
// binary_data_id in the queued tabular reading. Audio and metadata live in
// separate Viam stores and link two ways: by binary_data_id and by capture_id tag
var SessionSensor = resource.NewModel("viam", "speech-to-text", "session-sensor")

func init() {
	resource.RegisterComponent(sensor.API, SessionSensor,
		resource.Registration[sensor.Sensor, *SessionSensorConfig]{
			Constructor: newSessionSensor,
		})
}

type SessionSensorConfig struct {
	// DatasetIDs are attached to each binary upload, so captured WAVs can
	// be routed into a named training/debug dataset in the Viam app.
	DatasetIDs []string `json:"dataset_ids,omitempty"`

	// MaxQueueSize is a soft cap on pending readings. When exceeded, the
	// oldest reading is dropped with a warning log. Default 1000.
	MaxQueueSize int `json:"max_queue_size,omitempty"`
}

func (cfg *SessionSensorConfig) Validate(path string) ([]string, []string, error) {
	if cfg.MaxQueueSize < 0 {
		return nil, nil, fmt.Errorf("%s: max_queue_size must be >= 0 (got %d)", path, cfg.MaxQueueSize)
	}
	for i, id := range cfg.DatasetIDs {
		if id == "" {
			return nil, nil, fmt.Errorf("%s: dataset_ids[%d] is empty", path, i)
		}
	}
	return nil, nil, nil
}

// SessionReading is the typed shape STT modules pass to PushSession via the
// in-process sink interface. The DoCommand push path accepts the same
// fields as a map[string]interface{} with WAV base64-encoded.
type SessionReading struct {
	CaptureID      string
	Transcript     string
	Confidence     float64
	CloseReason    string
	ErrorMessage   string // populated when close_reason is an error path; empty otherwise
	AudioSentBytes int
	WAV            []byte // raw WAV bytes; uploaded to binary store, NOT stored in the tabular reading
	StartTime      time.Time
	EndTimeUs      int64 // session-close time, Unix microseconds
	LanguageCode   string
	Model          string
	ResponseCount  int
	FinalCount     int
	InterimCount   int
}

// SessionSensorSink is the in-process push API for STT models in this repo.
// Cross-repo STT modules use the DoCommand("push_session", ...) path instead.
type SessionSensorSink interface {
	PushSession(ctx context.Context, r SessionReading) (binaryDataID string, err error)
}

// binaryUploader is the subset of app.DataClient the sensor uses to upload
// session WAVs. Pulled into an interface so tests can stub it without
// constructing a real Viam app client.
type binaryUploader interface {
	BinaryDataCaptureUpload(
		ctx context.Context,
		binaryData []byte,
		partID string,
		componentType string,
		componentName string,
		methodName string,
		fileExtension string,
		options *app.BinaryDataCaptureUploadOptions,
	) (string, error)
}

type sessionSensor struct {
	resource.AlwaysRebuild

	name   resource.Name
	logger logging.Logger
	cfg    *SessionSensorConfig

	mu      sync.Mutex
	pending []map[string]interface{}
	// lastReading is the most recent row pushed. Non-data-manager callers
	// (Viam app live preview, Test panel, manual SDK calls) get this on
	// Readings() so the UI doesn't flicker; data-manager polls keep strict
	// queue-pop semantics so each row is captured exactly once.
	lastReading map[string]interface{}

	viamClient *app.ViamClient
	uploader   binaryUploader // nil disables binary upload; tabular queue still works
	partID     string
}

const (
	defaultMaxQueueSize = 1000

	envPartID = "VIAM_MACHINE_PART_ID"

	uploadMethod = "Readings"
	uploadCType  = "rdk:component:sensor"
	uploadFExt   = ".wav"
)

func newSessionSensor(ctx context.Context, _ resource.Dependencies, rawConf resource.Config, logger logging.Logger) (sensor.Sensor, error) {
	cfg, err := resource.NativeConfig[*SessionSensorConfig](rawConf)
	if err != nil {
		return nil, err
	}
	if cfg.MaxQueueSize <= 0 {
		cfg.MaxQueueSize = defaultMaxQueueSize
	}

	s := &sessionSensor{
		name:   rawConf.ResourceName(),
		logger: logger,
		cfg:    cfg,
	}

	// Lazily attempt to construct the Viam app client. If env vars are
	// missing (e.g. running locally without VIAM_API_KEY set), the sensor
	// still runs in "tabular-only" mode — binary uploads are skipped and
	// queued readings carry an empty binary_data_id.
	viamClient, err := app.CreateViamClientFromEnvVars(ctx, nil, logger)
	if err != nil {
		logger.Errorf("session-sensor: Viam app client unavailable (%v) — binary uploads disabled, tabular queue still works", err)
	} else {
		s.viamClient = viamClient
		s.uploader = viamClient.DataClient()
		s.partID = os.Getenv(envPartID)
		if s.partID == "" {
			logger.Warnf("session-sensor: %s not set — binary uploads disabled", envPartID)
			s.uploader = nil
		} else {
			logger.Infof("session-sensor: ready with binary uploads enabled (part_id=%s)", s.partID)
		}
	}

	return s, nil
}

func (s *sessionSensor) Name() resource.Name { return s.name }

func (s *sessionSensor) Status(context.Context) (map[string]interface{}, error) {
	return map[string]interface{}{}, nil
}

func (s *sessionSensor) Readings(_ context.Context, extra map[string]interface{}) (map[string]interface{}, error) {
	fromDM, _ := extra[data.FromDMString].(bool)
	s.mu.Lock()
	defer s.mu.Unlock()
	if fromDM {
		if len(s.pending) == 0 {
			return nil, data.ErrNoCaptureToStore
		}
		payload := s.pending[0]
		s.pending[0] = nil
		s.pending = s.pending[1:]
		return payload, nil
	}
	if s.lastReading != nil {
		return s.lastReading, nil
	}
	return nil, data.ErrNoCaptureToStore
}

// DoCommand handles cross-process push from STT modules that can't import
// the SessionSensorSink Go interface.
func (s *sessionSensor) DoCommand(ctx context.Context, cmd map[string]interface{}) (map[string]interface{}, error) {
	command, _ := cmd["command"].(string)
	switch command {
	case "push_session":
		r, err := parsePushPayload(cmd)
		if err != nil {
			return nil, fmt.Errorf("push_session: %w", err)
		}
		binID, err := s.PushSession(ctx, r)
		if err != nil {
			return map[string]interface{}{"capture_id": r.CaptureID, "binary_data_id": binID}, err
		}
		return map[string]interface{}{"capture_id": r.CaptureID, "binary_data_id": binID}, nil
	default:
		return nil, fmt.Errorf("unknown command %q (supported: push_session)", command)
	}
}

// PushSession is the in-process push entry point. It performs the binary
// upload (if a data client is configured), then queues the tabular reading
// with the resulting binary_data_id. Always returns the binary_data_id (may
// be empty on upload failure or when no data client is available) — the
// tabular reading is queued regardless so the failure is still observable in
// the Data tab.
func (s *sessionSensor) PushSession(ctx context.Context, r SessionReading) (string, error) {
	binID := ""
	var uploadErr error

	if s.uploader != nil && len(r.WAV) > 0 {
		tags := []string{
			"voice_session",
			"capture_" + r.CaptureID,
			closeReasonTag(r.CloseReason),
		}
		times := [2]time.Time{r.StartTime, time.UnixMicro(r.EndTimeUs)}
		opts := &app.BinaryDataCaptureUploadOptions{
			Tags:             tags,
			DatasetIDs:       s.cfg.DatasetIDs,
			DataRequestTimes: &times,
		}
		binID, uploadErr = s.uploader.BinaryDataCaptureUpload(
			ctx, r.WAV, s.partID, uploadCType, s.name.Name, uploadMethod, uploadFExt, opts,
		)
		if uploadErr != nil {
			s.logger.Errorf("session-sensor: binary upload failed for capture_id=%s: %v", r.CaptureID, uploadErr)
			binID = ""
		}
	} else if s.uploader == nil {
		s.logger.Debugf("session-sensor: no data client — skipping binary upload for capture_id=%s", r.CaptureID)
	}

	reading := map[string]interface{}{
		"capture_id":       r.CaptureID,
		"binary_data_id":   binID,
		"transcript":       r.Transcript,
		"confidence":       r.Confidence,
		"close_reason":     r.CloseReason,
		"error_message":    r.ErrorMessage,
		"audio_sent_bytes": r.AudioSentBytes,
		"start_time":       r.StartTime.UTC().Format(time.RFC3339Nano),
		"end_time_us":      r.EndTimeUs,
		"duration_ms":      float64(r.EndTimeUs-r.StartTime.UnixMicro()) / 1000,
		"language_code":    r.LanguageCode,
		"model":            r.Model,
		"response_count":   r.ResponseCount,
		"final_count":      r.FinalCount,
		"interim_count":    r.InterimCount,
		"captured_at":      time.Now().UTC().Format(time.RFC3339Nano),
	}

	s.mu.Lock()
	s.pending = append(s.pending, reading)
	s.lastReading = reading
	if over := len(s.pending) - s.cfg.MaxQueueSize; over > 0 {
		s.pending = s.pending[over:]
		s.logger.Warnf("session-sensor: queue over MaxQueueSize=%d, dropped %d oldest reading(s)", s.cfg.MaxQueueSize, over)
	}
	depth := len(s.pending)
	s.mu.Unlock()
	s.logger.Debugf("session-sensor: queued reading capture_id=%s binary_data_id=%q close_reason=%s queue_depth=%d", r.CaptureID, binID, r.CloseReason, depth)

	return binID, uploadErr
}

func (s *sessionSensor) Close(context.Context) error {
	if s.viamClient != nil {
		s.viamClient.Close()
	}
	return nil
}

// closeReasonTag buckets the typed close_reason into one of three coarse
// tags for filtering binary records in the Data tab UI.
func closeReasonTag(reason string) string {
	switch reason {
	case "success":
		return "success"
	case "no_result":
		return "no_result"
	default:
		return "error"
	}
}

// parsePushPayload extracts SessionReading fields from a DoCommand payload.
// audio_wav_b64 is base64-decoded into raw WAV bytes for the binary upload.
func parsePushPayload(cmd map[string]interface{}) (SessionReading, error) {
	var r SessionReading
	r.CaptureID, _ = cmd["capture_id"].(string)
	if r.CaptureID == "" {
		return r, fmt.Errorf("capture_id is required")
	}
	r.Transcript, _ = cmd["transcript"].(string)
	r.Confidence, _ = cmd["confidence"].(float64)
	r.CloseReason, _ = cmd["close_reason"].(string)
	if r.CloseReason == "" {
		return r, fmt.Errorf("close_reason is required")
	}
	r.ErrorMessage, _ = cmd["error_message"].(string)
	if n, ok := cmd["audio_sent_bytes"].(float64); ok {
		r.AudioSentBytes = int(n)
	}
	if b64, ok := cmd["audio_wav_b64"].(string); ok && b64 != "" {
		bytes, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return r, fmt.Errorf("audio_wav_b64 decode: %w", err)
		}
		r.WAV = bytes
	}
	if t, err := parseTimeField(cmd, "start_time"); err == nil {
		r.StartTime = t
	} else {
		return r, fmt.Errorf("start_time: %w", err)
	}
	if n, ok := cmd["end_time_us"].(float64); ok {
		r.EndTimeUs = int64(n)
	} else {
		return r, fmt.Errorf("end_time_us is required")
	}
	r.LanguageCode, _ = cmd["language_code"].(string)
	r.Model, _ = cmd["model"].(string)
	if n, ok := cmd["response_count"].(float64); ok {
		r.ResponseCount = int(n)
	}
	if n, ok := cmd["final_count"].(float64); ok {
		r.FinalCount = int(n)
	}
	if n, ok := cmd["interim_count"].(float64); ok {
		r.InterimCount = int(n)
	}
	return r, nil
}

func parseTimeField(cmd map[string]interface{}, key string) (time.Time, error) {
	raw, ok := cmd[key].(string)
	if !ok || raw == "" {
		return time.Time{}, fmt.Errorf("missing or non-string")
	}
	return time.Parse(time.RFC3339Nano, raw)
}
