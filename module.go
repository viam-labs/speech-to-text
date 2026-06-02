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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	speech "cloud.google.com/go/speech/apiv2"
	speechpb "cloud.google.com/go/speech/apiv2/speechpb"
	"google.golang.org/api/option"

	audioin "go.viam.com/rdk/components/audioin"
	generic "go.viam.com/rdk/components/generic"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
	genericservice "go.viam.com/rdk/services/generic"
	"go.viam.com/rdk/spatialmath"
	goutils "go.viam.com/utils"
)

var (
	GoogleCloudStt   = resource.NewModel("viam", "speech-to-text", "google-cloud-stt")
	errUnimplemented = errors.New("unimplemented")
)

func init() {
	resource.RegisterComponent(generic.API, GoogleCloudStt,
		resource.Registration[resource.Resource, *Config]{
			Constructor: newSpeechToTextGoogleCloudStt,
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

	// SampleRateHertz of the input audio. Defaults to 16000.
	SampleRateHertz int32 `json:"sample_rate_hertz,omitempty"`

	// MaxSessionSeconds caps a single Google streaming session. Google's
	// hard cap is 305s; defaults to 290s.
	MaxSessionSeconds int `json:"max_session_seconds,omitempty"`
}

// Validate declares dependencies and validates required fields.
func (cfg *Config) Validate(path string) ([]string, []string, error) {
	if cfg.Mic == "" {
		return nil, nil, fmt.Errorf("%s: mic is required", path)
	}
	if len(cfg.GoogleCredentialsJSON) == 0 {
		return nil, nil, fmt.Errorf("%s: google_credentials_json is required", path)
	}
	deps := []string{cfg.Mic}
	if cfg.TranscriptTarget != "" {
		deps = append(deps, cfg.TranscriptTarget)
	}
	return deps, nil, nil
}

type speechToTextGoogleCloudStt struct {
	resource.AlwaysRebuild
	resource.Named

	name resource.Name

	logger logging.Logger
	cfg    *Config

	audioIn          audioin.AudioIn
	transcriptTarget resource.Resource // generic resource, may be nil in log-only mode
	speechClient     *speech.Client
	projectID        string

	workers *goutils.StoppableWorkers
}

func newSpeechToTextGoogleCloudStt(ctx context.Context, deps resource.Dependencies, rawConf resource.Config, logger logging.Logger) (resource.Resource, error) {
	conf, err := resource.NativeConfig[*Config](rawConf)
	if err != nil {
		return nil, err
	}
	return NewGoogleCloudStt(ctx, deps, rawConf.ResourceName(), conf, logger)
}

// NewGoogleCloudStt is the typed constructor. Resolves deps, builds the Google
// client, and spawns the background listener.
func NewGoogleCloudStt(ctx context.Context, deps resource.Dependencies, name resource.Name, conf *Config, logger logging.Logger) (resource.Resource, error) {
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

	var clientOpts []option.ClientOption
	if len(conf.GoogleCredentialsJSON) > 0 {
		credBytes, err := json.Marshal(conf.GoogleCredentialsJSON)
		if err != nil {
			return nil, fmt.Errorf("marshal google_credentials_json: %w", err)
		}
		clientOpts = append(clientOpts, option.WithCredentialsJSON(credBytes))
	}
	speechClient, err := speech.NewClient(ctx, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("create google speech client: %w", err)
	}

	s := &speechToTextGoogleCloudStt{
		name:             name,
		logger:           logger,
		cfg:              conf,
		audioIn:          audioInResource,
		transcriptTarget: transcriptTarget,
		speechClient:     speechClient,
		projectID:        projectID,
	}
	s.workers = goutils.NewBackgroundStoppableWorkers(s.runListener)

	mode := "log-only"
	if transcriptTarget != nil {
		mode = "callback → " + conf.TranscriptTarget
	}
	logger.Infof("speech-to-text module ready: mic=%s lang=%s model=%s mode=%s",
		conf.Mic, conf.LanguageCode, conf.Model, mode)

	return s, nil
}

func (s *speechToTextGoogleCloudStt) Name() resource.Name {
	return s.name
}

// DoCommand exposes a minimal status interface for debugging. The module's
// real output flows to the configured transcript_target — there's no need for
// consumers to poll this module.
func (s *speechToTextGoogleCloudStt) DoCommand(ctx context.Context, cmd map[string]interface{}) (map[string]interface{}, error) {
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
func (s *speechToTextGoogleCloudStt) runListener(ctx context.Context) {
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

// drainAudio reads chunks from a single audio_in channel until it closes or
// the module shuts down. Manages the lifecycle of one Google session per
// audio segment.
func (s *speechToTextGoogleCloudStt) drainAudio(ctx context.Context, audioChan <-chan *audioin.AudioChunk) {
	var (
		gStream    speechpb.Speech_StreamingRecognizeClient
		gStreamCtx context.Context
		gCancel    context.CancelFunc
		recvDone   chan struct{}
	)

	closeGoogleSession := func(reason string) {
		if gStream == nil {
			return
		}
		_ = gStream.CloseSend()
		<-recvDone
		gCancel()
		s.logger.Infof("session closed (%s)", reason)
		gStream = nil
		gStreamCtx = nil
		gCancel = nil
		recvDone = nil
	}
	defer closeGoogleSession("audio_in channel closed")

	for {
		select {
		case <-ctx.Done():
			return
		case chunk, ok := <-audioChan:
			if !ok {
				return // audio_in stream ended; outer loop will reopen
			}
			if len(chunk.AudioData) == 0 {
				// Segment-end sentinel from filter-mic.
				closeGoogleSession("segment-end sentinel")
				continue
			}
			if gStream == nil {
				// Lazily open a Google session on first non-empty chunk.
				gStreamCtx, gCancel = context.WithTimeout(ctx, time.Duration(s.cfg.MaxSessionSeconds)*time.Second)
				st, err := s.speechClient.StreamingRecognize(gStreamCtx)
				if err != nil {
					s.logger.Warnf("open google stream: %v", err)
					gCancel()
					gStream = nil
					gStreamCtx = nil
					gCancel = nil
					continue
				}
				configReq := &speechpb.StreamingRecognizeRequest{
					Recognizer: fmt.Sprintf(
						"projects/%s/locations/global/recognizers/_",
						s.projectID,
					),
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
				if err := st.Send(configReq); err != nil {
					s.logger.Warnf("send config: %v", err)
					gCancel()
					gStream = nil
					gStreamCtx = nil
					gCancel = nil
					continue
				}
				gStream = st
				recvDone = make(chan struct{})
				go s.receiveFromGoogle(ctx, st, recvDone)
				s.logger.Infof("session opened (first audio chunk)")
			}

			audioReq := &speechpb.StreamingRecognizeRequest{
				StreamingRequest: &speechpb.StreamingRecognizeRequest_Audio{
					Audio: chunk.AudioData,
				},
			}
			if err := gStream.Send(audioReq); err != nil {
				s.logger.Warnf("send audio: %v", err)
				closeGoogleSession("send error")
			}
		}
	}
}

// receiveFromGoogle drains Google's response stream. For every final
// transcript, dispatches to the configured transcript_target (or logs).
func (s *speechToTextGoogleCloudStt) receiveFromGoogle(ctx context.Context, gStream speechpb.Speech_StreamingRecognizeClient, done chan struct{}) {
	defer close(done)
	for {
		resp, err := gStream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			if ctx.Err() == nil {
				s.logger.Warnf("google recv: %v", err)
			}
			return
		}
		for _, result := range resp.Results {
			if !result.IsFinal || len(result.Alternatives) == 0 {
				continue
			}
			text := result.Alternatives[0].Transcript
			conf := result.Alternatives[0].Confidence
			s.deliverFinal(ctx, text, conf)
		}
	}
}

// deliverFinal dispatches one final transcript. If transcript_target is
// configured, calls its DoCommand. Otherwise just logs.
func (s *speechToTextGoogleCloudStt) deliverFinal(ctx context.Context, text string, confidence float32) {
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

func (s *speechToTextGoogleCloudStt) Geometries(ctx context.Context, extra map[string]interface{}) ([]spatialmath.Geometry, error) {
	return nil, errUnimplemented
}

func (s *speechToTextGoogleCloudStt) Close(_ context.Context) error {
	s.workers.Stop()
	if s.speechClient != nil {
		return s.speechClient.Close()
	}
	return nil
}
