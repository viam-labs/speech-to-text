// Package google implements the google-cloud-stt model: a Transcriber backed by
// Google Cloud Speech v2 streaming recognition. The provider-agnostic mic /
// segment / dispatch / capture plumbing lives in the utils package.
package google

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	speech "cloud.google.com/go/speech/apiv2"
	speechpb "cloud.google.com/go/speech/apiv2/speechpb"
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

	"speechtotext/utils"
)

var (
	// Model is the model triple for the google-cloud-stt component.
	Model            = resource.NewModel("viam", "speech-to-text", "google-cloud-stt")
	errUnimplemented = errors.New("unimplemented")
)

func init() {
	resource.RegisterComponent(generic.API, Model,
		resource.Registration[resource.Resource, *Config]{
			Constructor: newGoogleCloudSTT,
		},
	)
}

// Config holds the google-cloud-stt model's machine.json attributes.
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

// googleCloudSTT is the generic resource for the google-cloud-stt model. It
// wires a googleTranscriber into the shared listener.
type googleCloudSTT struct {
	resource.AlwaysRebuild
	resource.Named

	name   resource.Name
	logger logging.Logger
	cfg    *Config

	transcriber *googleTranscriber
	listener    *utils.Listener
}

func newGoogleCloudSTT(ctx context.Context, deps resource.Dependencies, rawConf resource.Config, logger logging.Logger) (resource.Resource, error) {
	conf, err := resource.NativeConfig[*Config](rawConf)
	if err != nil {
		return nil, err
	}
	return NewGoogleCloudSTT(ctx, deps, rawConf.ResourceName(), conf, logger)
}

// NewGoogleCloudSTT applies defaults, resolves dependencies, builds the Google
// speech client, and spawns the background listener.
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

	var sessionSink utils.SessionSensorSink
	if conf.SessionSensorName != "" {
		sensorRes, err := sensor.FromDependencies(deps, conf.SessionSensorName)
		if err != nil {
			return nil, fmt.Errorf("session_sensor_name %q not found: %w", conf.SessionSensorName, err)
		}
		sink, ok := sensorRes.(utils.SessionSensorSink)
		if !ok {
			return nil, fmt.Errorf("session_sensor_name %q does not implement utils.SessionSensorSink (got %T) — must be a viam:speech-to-text:session-sensor", conf.SessionSensorName, sensorRes)
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

	gt := &googleTranscriber{
		logger:       logger,
		cfg:          conf,
		speechClient: speechClient,
		projectID:    projectID,
	}
	l := &utils.Listener{
		Logger:           logger,
		AudioIn:          audioInResource,
		TranscriptTarget: transcriptTarget,
		TargetName:       conf.TranscriptTarget,
		SessionSink:      sessionSink,
		SampleRate:       conf.SampleRateHertz,
		Source:           "google-cloud-stt",
		Transcriber:      gt,
	}
	s := &googleCloudSTT{
		name:        name,
		logger:      logger,
		cfg:         conf,
		transcriber: gt,
		listener:    l,
	}
	l.Start()

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
			"log_only":          s.listener.TranscriptTarget == nil,
		}, nil
	default:
		return nil, fmt.Errorf("unknown command %q (supported: status)", command)
	}
}

func (s *googleCloudSTT) Geometries(ctx context.Context, extra map[string]interface{}) ([]spatialmath.Geometry, error) {
	return nil, errUnimplemented
}

func (s *googleCloudSTT) Close(_ context.Context) error {
	s.listener.Stop()
	return s.transcriber.Close()
}

// googleTranscriber is the Google Cloud STT backend. It opens one streaming
// recognize RPC per audio segment.
type googleTranscriber struct {
	logger       logging.Logger
	cfg          *Config
	speechClient *speech.Client
	projectID    string
}

// OpenSession opens a Google streaming RPC, sends the initial config, and spawns
// the receive goroutine. deliver is called for each final transcript.
func (gt *googleTranscriber) OpenSession(ctx context.Context, deliver utils.DeliverFunc) (utils.ProviderSession, error) {
	streamCtx, cancel := context.WithTimeout(ctx, time.Duration(gt.cfg.MaxSessionSeconds)*time.Second)
	stream, err := gt.speechClient.StreamingRecognize(streamCtx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("open google stream: %w", err)
	}
	if err := stream.Send(gt.configRequest()); err != nil {
		cancel()
		return nil, fmt.Errorf("send config: %w", err)
	}
	sess := &sessionState{startedAt: time.Now()}
	recvDone := make(chan struct{})
	go gt.receiveFromGoogle(ctx, stream, sess, recvDone, deliver)
	return &googleSession{
		gt:       gt,
		stream:   stream,
		cancel:   cancel,
		recvDone: recvDone,
		state:    sess,
	}, nil
}

func (gt *googleTranscriber) Close() error {
	if gt.speechClient != nil {
		return gt.speechClient.Close()
	}
	return nil
}

// configRequest builds the initial StreamingRecognize config message.
func (gt *googleTranscriber) configRequest() *speechpb.StreamingRecognizeRequest {
	features := &speechpb.StreamingRecognitionFeatures{
		InterimResults: false,
	}
	return &speechpb.StreamingRecognizeRequest{
		Recognizer: fmt.Sprintf("projects/%s/locations/%s/recognizers/_", gt.projectID, gt.cfg.Location),
		StreamingRequest: &speechpb.StreamingRecognizeRequest_StreamingConfig{
			StreamingConfig: &speechpb.StreamingRecognitionConfig{
				Config: &speechpb.RecognitionConfig{
					DecodingConfig: &speechpb.RecognitionConfig_ExplicitDecodingConfig{
						ExplicitDecodingConfig: &speechpb.ExplicitDecodingConfig{
							Encoding:          speechpb.ExplicitDecodingConfig_LINEAR16,
							SampleRateHertz:   gt.cfg.SampleRateHertz,
							AudioChannelCount: 1,
						},
					},
					LanguageCodes: []string{gt.cfg.LanguageCode},
					Model:         gt.cfg.Model,
				},
				StreamingFeatures: features,
			},
		},
	}
}

// receiveFromGoogle drains Google's response stream. For every final transcript
// with usable text, it calls deliver. Counters and the latest
// transcript/confidence are recorded on sess so Finish can include them in the
// session-sensor push at close time.
func (gt *googleTranscriber) receiveFromGoogle(ctx context.Context, gStream speechpb.Speech_StreamingRecognizeClient, sess *sessionState, done chan struct{}, deliver utils.DeliverFunc) {
	defer close(done)
	defer func() {
		sess.mu.Lock()
		gt.logger.Debugf("google recv done: responses=%d finals=%d interims=%d", sess.respCount, sess.finalCount, sess.interimCount)
		sess.mu.Unlock()
	}()
	for {
		resp, err := gStream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			if ctx.Err() == nil {
				gt.logger.Warnf("google recv: %v", err)
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
			// models sometimes rank an empty hypothesis first, or emit
			// a final with no text. skip any finals with no text so
			// finalCount stays 0 and the session closes as no_result
			// instead of success.
			text, conf := firstNonEmptyAlternative(result.Alternatives)
			if text == "" {
				continue
			}
			sess.mu.Lock()
			sess.finalCount++
			sess.latestTranscript = text
			sess.latestConfidence = float64(conf)
			sess.mu.Unlock()
			// Time from session open to the final landing — the Google response latency.
			gt.logger.Debugf("google final transcript after %dms", time.Since(sess.startedAt).Milliseconds())
			deliver(text, float64(conf))
		}
	}
}

// firstNonEmptyAlternative returns the highest-ranked alternative with a
// non-blank transcript, or "" if no alternative has usable text.
func firstNonEmptyAlternative(alts []*speechpb.SpeechRecognitionAlternative) (string, float32) {
	for _, alt := range alts {
		if strings.TrimSpace(alt.Transcript) != "" {
			return alt.Transcript, alt.Confidence
		}
	}
	return "", 0
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

// sessionState holds per-Google-session state shared between the receive
// goroutine and Finish. The recv-side fields (latest transcript / confidence +
// counters) are written by receiveFromGoogle and read by Finish at session
// close, so they sit behind mu. recvErr/sendErr are single-writer.
type sessionState struct {
	startedAt time.Time
	recvErr   error // last non-EOF error from gStream.Recv, if any
	sendErr   error // last error from gStream.Send, if any

	mu               sync.Mutex
	latestTranscript string
	latestConfidence float64
	respCount        int
	finalCount       int
	interimCount     int
}

// googleSession is one Google streaming RPC, scoped to a single audio segment.
type googleSession struct {
	gt       *googleTranscriber
	stream   speechpb.Speech_StreamingRecognizeClient
	cancel   context.CancelFunc
	recvDone chan struct{}
	state    *sessionState
}

func (gs *googleSession) SendAudio(_ context.Context, pcm []byte) error {
	err := gs.stream.Send(&speechpb.StreamingRecognizeRequest{
		StreamingRequest: &speechpb.StreamingRecognizeRequest_Audio{Audio: pcm},
	})
	// io.EOF is the expected race when Google closed the stream first; the
	// listener maps it to "recv died". Record only genuine send failures.
	if err != nil && err != io.EOF {
		gs.state.sendErr = err
	}
	return err
}

func (gs *googleSession) Done() <-chan struct{} {
	return gs.recvDone
}

// Finish closes the Google stream, waits for the receive goroutine, normalizes
// the close reason, and returns the transcription-derived session fields.
func (gs *googleSession) Finish(_ context.Context, reason string) (utils.SessionReading, error) {
	_ = gs.stream.CloseSend()
	<-gs.recvDone
	gs.cancel()

	normalized := normalizeCloseReason(reason, gs.state)

	gs.state.mu.Lock()
	transcript := gs.state.latestTranscript
	confidence := gs.state.latestConfidence
	respCount := gs.state.respCount
	finalCount := gs.state.finalCount
	interimCount := gs.state.interimCount
	gs.state.mu.Unlock()

	// Surface the underlying gRPC error for recv_error / send_error closes.
	errorMessage := ""
	switch normalized {
	case "recv_error":
		if gs.state.recvErr != nil {
			errorMessage = gs.state.recvErr.Error()
		}
	case "send_error":
		if gs.state.sendErr != nil {
			errorMessage = gs.state.sendErr.Error()
		}
	}

	return utils.SessionReading{
		Transcript:    transcript,
		Confidence:    confidence,
		CloseReason:   normalized,
		ErrorMessage:  errorMessage,
		LanguageCode:  gs.gt.cfg.LanguageCode,
		Model:         gs.gt.cfg.Model,
		ResponseCount: respCount,
		FinalCount:    finalCount,
		InterimCount:  interimCount,
	}, nil
}

func (gs *googleSession) Close() {
	gs.cancel()
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
	// VoiceActivityTimeout causes Google to close the stream server-side after
	// emitting a final, so drainAudio sees recvDone before the sentinel arrives
	// and calls finish("recv died"). With no recv error and a final in hand,
	// this is a clean success — not a failure.
	case reason == "recv died" && recvErr == nil && finals > 0:
		return "success"
	case reason == "recv died" && finals == 0:
		return "no_result"
	case reason == "audio_in channel closed":
		return "context_cancelled"
	case reason == "send error":
		return "send_error"
	}
	return reason
}

// ensure googleSession satisfies ProviderSession and googleTranscriber
// satisfies Transcriber at compile time.
var (
	_ utils.ProviderSession = (*googleSession)(nil)
	_ utils.Transcriber     = (*googleTranscriber)(nil)
)
