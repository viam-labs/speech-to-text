// Package speechtotext implements a Viam generic resource that performs
// streaming speech-to-text via Google Cloud STT. It reads audio chunks from a
// configured audio_in dependency (e.g. viam-wake-filter), streams them to
// Google, and pushes final transcripts into a configured transcript_target
// resource via DoCommand.
//
// Wire shape (inverted callback):
//
//  1. Module starts. Spawns a background listener on the audio_in source.
//  2. When audio_in emits audio chunks (e.g. filter-mic detected its wake
//     word), module opens a Google streaming session and forwards chunks.
//  3. When Google returns a final transcript, module calls
//     transcript_target.DoCommand({
//     "command": "deliverTranscript",
//     "transcript": "...",
//     "is_final": true,
//     "confidence": 0.92,
//     "source": "google-cloud-stt",
//     })
//  4. When audio_in emits its segment-end sentinel (empty AudioChunk),
//     module closes the Google session and goes back to waiting.
//
// If transcript_target is unset (testing mode), the module just logs finals
// to stdout instead of calling back. Useful for local validation.
package speechtotext

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	speech "cloud.google.com/go/speech/apiv2"
	speechpb "cloud.google.com/go/speech/apiv2/speechpb"
	"github.com/google/uuid"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	audioin "go.viam.com/rdk/components/audioin"
	generic "go.viam.com/rdk/components/generic"
	"go.viam.com/rdk/components/sensor"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
	genericservice "go.viam.com/rdk/services/generic"
	"go.viam.com/rdk/spatialmath"
	rutils "go.viam.com/rdk/utils"
	goutils "go.viam.com/utils"
)

var (
	GoogleCloudSTT   = resource.NewModel("viam", "speech-to-text", "google-cloud-stt")
	errUnimplemented = errors.New("unimplemented")
)

func init() {
	resource.RegisterComponent(generic.API, GoogleCloudSTT,
		resource.Registration[resource.Resource, *Config]{
			Constructor: newGoogleCloudSTT,
		},
	)
}

// Config holds the module's machine.json attributes.
type Config struct {
	// Mic is the Viam resource name of an audio_in component that emits
	// PCM16 16kHz mono audio (e.g. a viam-wake-filter instance).
	Mic string `json:"mic"`

	// TranscriptTarget is the Viam resource name of a generic component
	// the module calls when it has a final transcript. The target must
	// expose a DoCommand handler accepting {"command": "deliverTranscript",
	// "transcript": "...", "is_final": true, ...}.
	//
	// If empty, the module runs in log-only mode: finals are logged but not
	// dispatched. Useful for local testing.
	TranscriptTarget string `json:"transcript_target,omitempty"`

	// GoogleCredentialsJSON is the inline GCP service-account JSON. Required.
	GoogleCredentialsJSON map[string]interface{} `json:"google_credentials_json"`

	// LanguageCode for recognition. Defaults to "en-US".
	LanguageCode string `json:"language_code,omitempty"`

	// Model picks the recognition model. v2 supports "latest_short" (default,
	// for commands), "latest_long", "chirp_2", etc.
	Model string `json:"model,omitempty"`

	// Location is the Google Cloud region for the Speech v2 recognizer.
	// Defaults to "global". Models like "chirp_2" are only available in
	// regional endpoints (e.g. "us-central1", "us-east1", "europe-west4").
	// When set to a non-global region, the client targets <location>-speech.googleapis.com.
	Location string `json:"location,omitempty"`

	// SampleRateHertz of the input audio. Defaults to 16000.
	SampleRateHertz int32 `json:"sample_rate_hertz,omitempty"`

	// MaxSessionSeconds caps a single Google streaming session. Google's
	// hard cap is 305s; defaults to 290s.
	MaxSessionSeconds int `json:"max_session_seconds,omitempty"`

	// SessionSensorName is the Viam resource name of a session-sensor
	// component that captures per-session WAV + metadata for the Data tab.
	// Optional — if empty, capture is disabled and the module runs as before.
	SessionSensorName string `json:"session_sensor_name,omitempty"`
}

// Validate declares dependencies and validates required fields.
func (cfg *Config) Validate(path string) ([]string, []string, error) {
	if cfg.Mic == "" {
		return nil, nil, fmt.Errorf("%s: mic is required", path)
	}
	deps := []string{cfg.Mic}
	if len(cfg.GoogleCredentialsJSON) == 0 {
		return nil, nil, fmt.Errorf("%s: google_credentials_json is required", path)
	}
	if cfg.TranscriptTarget != "" {
		deps = append(deps, cfg.TranscriptTarget)
	}
	if cfg.SessionSensorName != "" {
		deps = append(deps, cfg.SessionSensorName)
	}
	return deps, nil, nil
}

type googleCloudSTT struct {
	resource.AlwaysRebuild
	resource.Named

	name resource.Name

	logger logging.Logger
	cfg    *Config

	audioIn          audioin.AudioIn
	transcriptTarget resource.Resource // generic resource, may be nil in log-only mode
	sessionSink      SessionSensorSink // nil if session_sensor_name unset
	speechClient     *speech.Client
	projectID        string

	workers *goutils.StoppableWorkers
}

func newGoogleCloudSTT(ctx context.Context, deps resource.Dependencies, rawConf resource.Config, logger logging.Logger) (resource.Resource, error) {
	conf, err := resource.NativeConfig[*Config](rawConf)
	if err != nil {
		return nil, err
	}
	return NewGoogleCloudSTT(ctx, deps, rawConf.ResourceName(), conf, logger)
}

// client, and spawns the background listener.
func NewGoogleCloudSTT(ctx context.Context, deps resource.Dependencies, name resource.Name, conf *Config, logger logging.Logger) (resource.Resource, error) {
	if conf.LanguageCode == "" {
		conf.LanguageCode = "en-US"
	}
	if conf.Model == "" {
		conf.Model = "latest_short"
	}
	if conf.SampleRateHertz == 0 {
		conf.SampleRateHertz = 16000
	}
	if conf.MaxSessionSeconds == 0 {
		conf.MaxSessionSeconds = 290
	}
	if conf.Location == "" {
		conf.Location = "global"
	}

	projectID, _ := conf.GoogleCredentialsJSON["project_id"].(string)
	if projectID == "" {
		return nil, fmt.Errorf("google_credentials_json missing project_id field")
	}

	audioInResource, err := audioin.FromProvider(deps, conf.Mic)
	if err != nil {
		return nil, fmt.Errorf("mic %q not found: %w", conf.Mic, err)
	}
	var transcriptTarget resource.Resource
	if conf.TranscriptTarget != "" {
		tgt, err := genericservice.FromProvider(deps, conf.TranscriptTarget)
		if err != nil {
			return nil, fmt.Errorf("transcript_target %q not found: %w", conf.TranscriptTarget, err)
		}
		transcriptTarget = tgt
	}

	var sessionSink SessionSensorSink
	if conf.SessionSensorName != "" {
		sensorRes, err := sensor.FromDependencies(deps, conf.SessionSensorName)
		if err != nil {
			return nil, fmt.Errorf("session_sensor_name %q not found: %w", conf.SessionSensorName, err)
		}
		sink, ok := sensorRes.(SessionSensorSink)
		if !ok {
			return nil, fmt.Errorf("session_sensor_name %q does not implement SessionSensorSink (got %T) — must be a viam:speech-to-text:session-sensor", conf.SessionSensorName, sensorRes)
		}
		sessionSink = sink
	}

	var clientOpts []option.ClientOption
	if len(conf.GoogleCredentialsJSON) > 0 {
		credBytes, err := json.Marshal(conf.GoogleCredentialsJSON)
		if err != nil {
			return nil, fmt.Errorf("marshal google_credentials_json: %w", err)
		}
		clientOpts = append(clientOpts, option.WithCredentialsJSON(credBytes))
	}
	if conf.Location != "global" {
		clientOpts = append(clientOpts, option.WithEndpoint(fmt.Sprintf("%s-speech.googleapis.com:443", conf.Location)))
	}
	speechClient, err := speech.NewClient(ctx, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("create google speech client: %w", err)
	}

	// Probe the configured model/language/location against Google up-front so a
	// bad combination fails the constructor instead of looping on every wake
	// word at runtime (e.g. "language en-US not supported by model long in
	// northamerica-northeast1").
	if err := probeGoogleConfig(ctx, speechClient, conf, projectID); err != nil {
		_ = speechClient.Close()
		return nil, err
	}

	s := &googleCloudSTT{
		name:             name,
		logger:           logger,
		cfg:              conf,
		audioIn:          audioInResource,
		transcriptTarget: transcriptTarget,
		sessionSink:      sessionSink,
		speechClient:     speechClient,
		projectID:        projectID,
	}
	s.workers = goutils.NewBackgroundStoppableWorkers(s.runListener)

	mode := "log-only"
	if transcriptTarget != nil {
		mode = "callback → " + conf.TranscriptTarget
	}
	sinkMode := "disabled"
	if sessionSink != nil {
		sinkMode = "→ " + conf.SessionSensorName
	}
	logger.Infof("speech-to-text module ready: mic=%s lang=%s model=%s mode=%s session_sensor=%s",
		conf.Mic, conf.LanguageCode, conf.Model, mode, sinkMode)

	return s, nil
}

func (s *googleCloudSTT) Name() resource.Name {
	return s.name
}

// DoCommand exposes a minimal status interface for debugging. The module's
// real output flows to the configured transcript_target — there's no need for
// consumers to poll this module.
func (s *googleCloudSTT) DoCommand(ctx context.Context, cmd map[string]interface{}) (map[string]interface{}, error) {
	command, _ := cmd["command"].(string)
	switch command {
	case "status":
		return map[string]interface{}{
			"transcript_target": s.cfg.TranscriptTarget,
			"log_only":          s.transcriptTarget == nil,
		}, nil
	default:
		return nil, fmt.Errorf("unknown command %q (supported: status)", command)
	}
}

// runListener is the single background goroutine that reads from filter-mic.
// When chunks start arriving (wake fired upstream), it opens a Google session
// and forwards them. When the empty-chunk sentinel arrives, it closes the
// session and loops back to waiting. Runs for the lifetime of the module.
func (s *googleCloudSTT) runListener(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		audioChan, err := s.audioIn.GetAudio(ctx, "pcm16", 0, 0, nil)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Warnf("audio_in.GetAudio: %v; retrying in 1s", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		// Drain chunks. Open a Google session lazily when first non-empty
		// chunk arrives. Close it on segment-end (empty chunk) or channel
		// close.
		s.drainAudio(ctx, audioChan)
	}
}

// probeGoogleConfig opens a short streaming session with the configured
// model/language/location and sends a tiny silent chunk to force server-side
// validation. Returns an error if Google rejects the config with a code that
// will never succeed (InvalidArgument, Unauthenticated, PermissionDenied,
// NotFound). Other errors (network, transient) are logged but allowed through
// — the runtime loop handles retries.
func probeGoogleConfig(ctx context.Context, client *speech.Client, conf *Config, projectID string) error {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	st, err := client.StreamingRecognize(probeCtx)
	if err != nil {
		return fmt.Errorf("probe stream open: %w", err)
	}

	configReq := &speechpb.StreamingRecognizeRequest{
		Recognizer: fmt.Sprintf("projects/%s/locations/%s/recognizers/_", projectID, conf.Location),
		StreamingRequest: &speechpb.StreamingRecognizeRequest_StreamingConfig{
			StreamingConfig: &speechpb.StreamingRecognitionConfig{
				Config: &speechpb.RecognitionConfig{
					DecodingConfig: &speechpb.RecognitionConfig_ExplicitDecodingConfig{
						ExplicitDecodingConfig: &speechpb.ExplicitDecodingConfig{
							Encoding:          speechpb.ExplicitDecodingConfig_LINEAR16,
							SampleRateHertz:   conf.SampleRateHertz,
							AudioChannelCount: 1,
						},
					},
					LanguageCodes: []string{conf.LanguageCode},
					Model:         conf.Model,
				},
			},
		},
	}
	if err := st.Send(configReq); err != nil {
		return fmt.Errorf("probe config send: %w", err)
	}
	// 10ms of silence at 16kHz mono PCM16 = 320 bytes.
	silence := make([]byte, int(conf.SampleRateHertz)*2/100)
	if err := st.Send(&speechpb.StreamingRecognizeRequest{
		StreamingRequest: &speechpb.StreamingRecognizeRequest_Audio{Audio: silence},
	}); err != nil {
		return fmt.Errorf("probe audio send: %w", err)
	}
	_ = st.CloseSend()

	for {
		_, err := st.Recv()
		if err == io.EOF {
			return nil
		}
		if err == nil {
			continue
		}
		if s, ok := status.FromError(err); ok {
			switch s.Code() {
			case codes.InvalidArgument, codes.Unauthenticated, codes.PermissionDenied, codes.NotFound:
				return fmt.Errorf("speech-to-text config rejected by Google (model=%q language=%q location=%q): %w",
					conf.Model, conf.LanguageCode, conf.Location, err)
			}
		}
		// Non-config error (network, deadline, etc.) — don't block startup.
		return nil
	}
}

// sessionState holds per-Google-session state shared between drainAudio (the
// sender side) and receiveFromGoogle (the receiver side). The send-side fields
// (captureID, startedAt, audioBuf, audioSent) are owned exclusively by
// drainAudio and need no locking; the recv-side fields (latest transcript /
// confidence + counters) are written by receiveFromGoogle and read by
// drainAudio at session close, so they sit behind mu.
type sessionState struct {
	captureID string
	startedAt time.Time
	audioBuf  bytes.Buffer
	audioSent int
	recvErr   error // last non-EOF error from gStream.Recv, if any
	sendErr   error // last error from gStream.Send, if any

	mu               sync.Mutex
	latestTranscript string
	latestConfidence float64
	respCount        int
	finalCount       int
	interimCount     int
}

// googleSession bundles all per-session resources whose lifetimes are tied
// to a single Google streaming RPC.
type googleSession struct {
	stream    speechpb.Speech_StreamingRecognizeClient
	streamCtx context.Context
	cancel    context.CancelFunc
	recvDone  chan struct{}
	state     *sessionState
}

// drainAudio reads chunks from a single audio_in channel until it closes or
// the module shuts down. Manages the lifecycle of one Google session per
// audio segment.
func (s *googleCloudSTT) drainAudio(ctx context.Context, audioChan <-chan *audioin.AudioChunk) {
	var session *googleSession
	defer func() {
		if session != nil {
			s.closeSession(ctx, session, "audio_in channel closed")
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case chunk, ok := <-audioChan:
			if !ok {
				return // audio_in stream ended; outer loop will reopen
			}
			if len(chunk.AudioData) == 0 {
				if session != nil {
					s.closeSession(ctx, session, "segment-end sentinel")
					session = nil
				}
				continue
			}
			if session == nil {
				opened, err := s.openSession(ctx)
				if err != nil {
					s.logger.Warnf("%v", err)
					continue
				}
				session = opened
			}
			audioReq := &speechpb.StreamingRecognizeRequest{
				StreamingRequest: &speechpb.StreamingRecognizeRequest_Audio{
					Audio: chunk.AudioData,
				},
			}
			if err := session.stream.Send(audioReq); err != nil {
				s.logger.Warnf("send audio: %v", err)
				session.state.sendErr = err
				s.closeSession(ctx, session, "send error")
				session = nil
				continue
			}
			session.state.audioSent += len(chunk.AudioData)
			session.state.audioBuf.Write(chunk.AudioData)
		}
	}
}

// openSession opens a Google streaming RPC, sends the initial config, and
// spawns the receive goroutine. Returns a ready-to-use session on success.
func (s *googleCloudSTT) openSession(ctx context.Context) (*googleSession, error) {
	streamCtx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.MaxSessionSeconds)*time.Second)
	stream, err := s.speechClient.StreamingRecognize(streamCtx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("open google stream: %w", err)
	}
	if err := stream.Send(s.configRequest()); err != nil {
		cancel()
		return nil, fmt.Errorf("send config: %w", err)
	}
	sess := &sessionState{
		captureID: uuid.NewString(),
		startedAt: time.Now(),
	}
	recvDone := make(chan struct{})
	go s.receiveFromGoogle(ctx, stream, sess, recvDone)
	s.logger.Debugf("session opened (first audio chunk) capture_id=%s", sess.captureID)
	return &googleSession{
		stream:    stream,
		streamCtx: streamCtx,
		cancel:    cancel,
		recvDone:  recvDone,
		state:     sess,
	}, nil
}

// closeSession closes the Google stream, waits for the receive goroutine,
// normalizes the close reason, logs, and pushes the session capture.
func (s *googleCloudSTT) closeSession(ctx context.Context, session *googleSession, reason string) {
	_ = session.stream.CloseSend()
	<-session.recvDone
	session.cancel()
	normalized := normalizeCloseReason(reason, session.state)
	s.logger.Debugf("session closed (%s) audio_sent=%d bytes", normalized, session.state.audioSent)
	s.pushSessionCapture(ctx, session.state, normalized)
}

// configRequest builds the initial StreamingRecognize config message.
func (s *googleCloudSTT) configRequest() *speechpb.StreamingRecognizeRequest {
	return &speechpb.StreamingRecognizeRequest{
		Recognizer: fmt.Sprintf("projects/%s/locations/%s/recognizers/_", s.projectID, s.cfg.Location),
		StreamingRequest: &speechpb.StreamingRecognizeRequest_StreamingConfig{
			StreamingConfig: &speechpb.StreamingRecognitionConfig{
				Config: &speechpb.RecognitionConfig{
					DecodingConfig: &speechpb.RecognitionConfig_ExplicitDecodingConfig{
						ExplicitDecodingConfig: &speechpb.ExplicitDecodingConfig{
							Encoding:          speechpb.ExplicitDecodingConfig_LINEAR16,
							SampleRateHertz:   s.cfg.SampleRateHertz,
							AudioChannelCount: 1,
						},
					},
					LanguageCodes: []string{s.cfg.LanguageCode},
					Model:         s.cfg.Model,
				},
				StreamingFeatures: &speechpb.StreamingRecognitionFeatures{
					// Finals only — consumer only needs final transcripts.
					InterimResults: false,
				},
			},
		},
	}
}

// normalizeCloseReason maps a raw close reason to one of the canonical
// telemetry buckets. A recv error trumps the original reason.
func normalizeCloseReason(reason string, sess *sessionState) string {
	sess.mu.Lock()
	finals := sess.finalCount
	recvErr := sess.recvErr
	sess.mu.Unlock()
	switch {
	case recvErr != nil:
		return "recv_error"
	case reason == "segment-end sentinel" && finals > 0:
		return "success"
	case reason == "segment-end sentinel" && finals == 0:
		return "no_result"
	case reason == "audio_in channel closed":
		return "context_cancelled"
	case reason == "send error":
		return "send_error"
	}
	return reason
}

// receiveFromGoogle drains Google's response stream. For every final
// transcript, dispatches to the configured transcript_target (or logs).
// Counters and the latest transcript/confidence are recorded on sess so the
// sender side can include them in the session-sensor push at close time.
func (s *googleCloudSTT) receiveFromGoogle(ctx context.Context, gStream speechpb.Speech_StreamingRecognizeClient, sess *sessionState, done chan struct{}) {
	defer close(done)
	defer func() {
		sess.mu.Lock()
		s.logger.Debugf("google recv done: responses=%d finals=%d interims=%d", sess.respCount, sess.finalCount, sess.interimCount)
		sess.mu.Unlock()
	}()
	for {
		resp, err := gStream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			if ctx.Err() == nil {
				s.logger.Warnf("google recv: %v", err)
				sess.recvErr = err
			}
			return
		}
		sess.mu.Lock()
		sess.respCount++
		sess.mu.Unlock()
		for _, result := range resp.Results {
			if len(result.Alternatives) == 0 {
				continue
			}
			if !result.IsFinal {
				sess.mu.Lock()
				sess.interimCount++
				sess.mu.Unlock()
				continue
			}
			text := result.Alternatives[0].Transcript
			conf := result.Alternatives[0].Confidence
			sess.mu.Lock()
			sess.finalCount++
			sess.latestTranscript = text
			sess.latestConfidence = float64(conf)
			sess.mu.Unlock()
			s.deliverFinal(ctx, text, conf)
		}
	}
}

// pushSessionCapture snapshots the session state and forwards it to the
// configured session-sensor sink. Runs in a goroutine so a slow upload never
// blocks the audio loop. No-op if no sink is configured.
func (s *googleCloudSTT) pushSessionCapture(ctx context.Context, sess *sessionState, closeReason string) {
	if s.sessionSink == nil {
		s.logger.Debugf("session-sensor not configured — skipping push for close_reason=%s", closeReason)
		return
	}
	if sess == nil || sess.captureID == "" {
		return
	}
	sess.mu.Lock()
	transcript := sess.latestTranscript
	confidence := sess.latestConfidence
	respCount := sess.respCount
	finalCount := sess.finalCount
	interimCount := sess.interimCount
	sess.mu.Unlock()

	// Surface the underlying gRPC error for recv_error / send_error close
	errorMessage := ""
	switch closeReason {
	case "recv_error":
		if sess.recvErr != nil {
			errorMessage = sess.recvErr.Error()
		}
	case "send_error":
		if sess.sendErr != nil {
			errorMessage = sess.sendErr.Error()
		}
	}

	wav, err := audioin.CreateWAVFile(sess.audioBuf.Bytes(), s.cfg.SampleRateHertz, 1, rutils.CodecPCM16)
	if err != nil {
		s.logger.Errorf("session-sensor: wrap PCM as WAV (capture_id=%s): %v", sess.captureID, err)
		wav = nil
	}
	r := SessionReading{
		CaptureID:      sess.captureID,
		Transcript:     transcript,
		Confidence:     confidence,
		CloseReason:    closeReason,
		ErrorMessage:   errorMessage,
		AudioSentBytes: sess.audioSent,
		WAV:            wav,
		StartTime:      sess.startedAt,
		EndTime:        time.Now(),
		LanguageCode:   s.cfg.LanguageCode,
		Model:          s.cfg.Model,
		ResponseCount:  respCount,
		FinalCount:     finalCount,
		InterimCount:   interimCount,
	}
	sink := s.sessionSink
	logger := s.logger
	go func() {
		// Detach from caller's ctx — the audio loop may be shutting down and don't want to stop the push.
		pushCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		binID, err := sink.PushSession(pushCtx, r)
		if err != nil {
			logger.Errorf("session-sensor push failed (capture_id=%s): %v", r.CaptureID, err)
			return
		}
		logger.Debugf("session-sensor push ok: capture_id=%s binary_data_id=%q close_reason=%s wav_bytes=%d", r.CaptureID, binID, r.CloseReason, len(r.WAV))
	}()
}

// deliverFinal dispatches one final transcript. If transcript_target is
// configured, calls its DoCommand. Otherwise just logs.
func (s *googleCloudSTT) deliverFinal(ctx context.Context, text string, confidence float32) {
	s.logger.Debugf("FINAL: %q (conf=%.2f)", text, confidence)
	if s.transcriptTarget == nil {
		return
	}
	doCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := s.transcriptTarget.DoCommand(doCtx, map[string]interface{}{
		"command":    "deliverTranscript",
		"transcript": text,
		"is_final":   true,
		"confidence": float64(confidence),
		"source":     "google-cloud-stt",
	})
	if err != nil {
		s.logger.Warnf("deliverTranscript to %s: %v",
			s.cfg.TranscriptTarget, err)
	}
}

func (s *googleCloudSTT) Geometries(ctx context.Context, extra map[string]interface{}) ([]spatialmath.Geometry, error) {
	return nil, errUnimplemented
}

func (s *googleCloudSTT) Close(_ context.Context) error {
	s.workers.Stop()
	if s.speechClient != nil {
		return s.speechClient.Close()
	}
	return nil
}
