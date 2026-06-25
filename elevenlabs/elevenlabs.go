// Package elevenlabs implements the elevenlabs-stt model: a Transcriber backed
// by the ElevenLabs Scribe realtime WebSocket API. The provider-agnostic mic /
// segment / dispatch / capture plumbing lives in the utils package.
//
// Protocol (mirrors the retired Python speech-to-text-11labs module):
//
//  1. A single-use token is fetched (POST /v1/single-use-token/realtime_scribe,
//     xi-api-key header), then a WebSocket is opened to
//     wss://api.elevenlabs.io/v1/speech-to-text/realtime?...&token=<token>.
//     The transcriber pre-connects the next session's WebSocket in the
//     background so a wake word pays near-zero open latency.
//  2. Each PCM16 chunk is base64-encoded and sent as
//     {"message_type":"input_audio_chunk","audio_base_64":"...","commit":false,
//     "sample_rate":16000}.
//  3. At segment end the same message is sent with "commit":true; the next
//     pre-connect is kicked off, and the session waits for the server's
//     committed_transcript response, then delivers it.
//  4. The WebSocket is closed after each committed transcript.
package elevenlabs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"sync"
	"time"

	"nhooyr.io/websocket"

	audioin "go.viam.com/rdk/components/audioin"
	generic "go.viam.com/rdk/components/generic"
	"go.viam.com/rdk/components/sensor"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
	genericservice "go.viam.com/rdk/services/generic"
	"go.viam.com/rdk/spatialmath"

	"speechtotext/utils"
)

// Model is the model triple for the elevenlabs-stt component.
var Model = resource.NewModel("viam", "speech-to-text", "elevenlabs-stt")

var errUnimplemented = errors.New("unimplemented")

const (
	elevenLabsWSBase          = "wss://api.elevenlabs.io/v1/speech-to-text/realtime"
	elevenLabsTokenURL        = "https://api.elevenlabs.io/v1/single-use-token/realtime_scribe"
	defaultElevenLabsModel    = "scribe_v2_realtime"
	defaultElevenLabsLanguage = "en"
)

func init() {
	resource.RegisterComponent(generic.API, Model,
		resource.Registration[resource.Resource, *elevenLabsConfig]{
			Constructor: newElevenLabsSTT,
		},
	)
}

// elevenLabsConfig holds the elevenlabs-stt model's machine.json attributes.
type elevenLabsConfig struct {
	utils.STTConfig `json:",squash"`

	// APIKey is the ElevenLabs API key. Required.
	APIKey string `json:"api_key"`

	// ModelID picks the Scribe model. Defaults to "scribe_v2_realtime".
	ModelID string `json:"model_id,omitempty"`
}

// Validate declares dependencies and validates required fields.
func (cfg *elevenLabsConfig) Validate(path string) ([]string, []string, error) {
	if cfg.Mic == "" {
		return nil, nil, fmt.Errorf("%s: mic is required", path)
	}
	if cfg.APIKey == "" {
		return nil, nil, fmt.Errorf("%s: api_key is required", path)
	}
	deps := []string{cfg.Mic}
	if cfg.TranscriptTarget != "" {
		deps = append(deps, cfg.TranscriptTarget)
	}
	if cfg.SessionSensorName != "" {
		deps = append(deps, cfg.SessionSensorName)
	}
	return deps, nil, nil
}

// elevenLabsSTT is the generic resource for the elevenlabs-stt model.
type elevenLabsSTT struct {
	resource.AlwaysRebuild
	resource.Named

	name   resource.Name
	logger logging.Logger
	cfg    *elevenLabsConfig

	transcriber *elevenLabsTranscriber
	listener    *utils.Listener
}

func newElevenLabsSTT(ctx context.Context, deps resource.Dependencies, rawConf resource.Config, logger logging.Logger) (resource.Resource, error) {
	conf, err := resource.NativeConfig[*elevenLabsConfig](rawConf)
	if err != nil {
		return nil, err
	}
	return NewElevenLabsSTT(ctx, deps, rawConf.ResourceName(), conf, logger)
}

// NewElevenLabsSTT applies defaults, resolves dependencies, probes the API key,
// and spawns the background listener.
func NewElevenLabsSTT(ctx context.Context, deps resource.Dependencies, name resource.Name, conf *elevenLabsConfig, logger logging.Logger) (resource.Resource, error) {
	if conf.ModelID == "" {
		conf.ModelID = defaultElevenLabsModel
	}
	if conf.LanguageCode == "" {
		conf.LanguageCode = defaultElevenLabsLanguage
	}
	if conf.SampleRateHertz == 0 {
		conf.SampleRateHertz = 16000
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

	bgCtx, bgCancel := context.WithCancel(context.Background())
	et := &elevenLabsTranscriber{
		logger:       logger,
		apiKey:       conf.APIKey,
		modelID:      conf.ModelID,
		languageCode: conf.LanguageCode,
		sampleRate:   conf.SampleRateHertz,
		httpClient:   &http.Client{Timeout: 10 * time.Second},
		bgCtx:        bgCtx,
		bgCancel:     bgCancel,
	}

	// Construction-level probe: fetch one single-use token so a bad api_key
	// fails the resource now instead of silently looping on every wake word.
	// Transient failures (network, 5xx) are logged but allowed through — the
	// runtime prefetch loop retries — mirroring Google's lenient probe.
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	_, err = et.getSingleUseToken(probeCtx)
	cancel()
	var authErr *elevenLabsAuthError
	if errors.As(err, &authErr) {
		bgCancel()
		return nil, fmt.Errorf("elevenlabs api_key rejected: %w", err)
	}
	if err != nil {
		logger.Warnf("elevenlabs token probe failed (%v) — continuing; runtime will retry", err)
	}

	l := &utils.Listener{
		Logger:           logger,
		AudioIn:          audioInResource,
		TranscriptTarget: transcriptTarget,
		TargetName:       conf.TranscriptTarget,
		SessionSink:      sessionSink,
		SampleRate:       conf.SampleRateHertz,
		Source:           "elevenlabs-stt",
		Transcriber:      et,
	}
	s := &elevenLabsSTT{
		name:        name,
		logger:      logger,
		cfg:         conf,
		transcriber: et,
		listener:    l,
	}
	// Warm a connection for the first wake word.
	et.schedulePrefetch()
	l.Start()

	mode := "log-only"
	if transcriptTarget != nil {
		mode = "callback → " + conf.TranscriptTarget
	}
	sinkMode := "disabled"
	if sessionSink != nil {
		sinkMode = "→ " + conf.SessionSensorName
	}
	logger.Infof("speech-to-text-11labs ready: mic=%s lang=%s model=%s mode=%s session_sensor=%s",
		conf.Mic, conf.LanguageCode, conf.ModelID, mode, sinkMode)

	return s, nil
}

func (s *elevenLabsSTT) Name() resource.Name {
	return s.name
}

func (s *elevenLabsSTT) DoCommand(ctx context.Context, cmd map[string]interface{}) (map[string]interface{}, error) {
	command, _ := cmd["command"].(string)
	switch command {
	case "status":
		return map[string]interface{}{
			"transcript_target": s.cfg.TranscriptTarget,
			"log_only":          s.listener.TranscriptTarget == nil,
			"model_id":          s.cfg.ModelID,
			"language_code":     s.cfg.LanguageCode,
		}, nil
	default:
		return nil, fmt.Errorf("unknown command %q (supported: status)", command)
	}
}

func (s *elevenLabsSTT) Geometries(ctx context.Context, extra map[string]interface{}) ([]spatialmath.Geometry, error) {
	return nil, errUnimplemented
}

func (s *elevenLabsSTT) Close(_ context.Context) error {
	s.listener.Stop()
	return s.transcriber.Close()
}

// elevenLabsTranscriber is the ElevenLabs Scribe backend. It owns the
// background pre-connect of the next session's WebSocket.
type elevenLabsTranscriber struct {
	logger       logging.Logger
	apiKey       string
	modelID      string
	languageCode string
	sampleRate   int32
	httpClient   *http.Client

	// bgCtx scopes background prefetch goroutines to the module's lifetime, so
	// they survive past any single request context but stop on Close.
	bgCtx    context.Context
	bgCancel context.CancelFunc

	mu         sync.Mutex
	prefetchCh chan wsResult // non-nil while a pre-connect is in flight or ready
}

type wsResult struct {
	ws  *websocket.Conn
	err error
}

// elevenLabsAuthError marks a token fetch rejected for credential reasons
// (401/403) — a permanent failure the constructor should surface — as distinct
// from transient errors (network, 5xx) that the runtime loop can retry.
type elevenLabsAuthError struct {
	status int
	body   string
}

func (e *elevenLabsAuthError) Error() string {
	return fmt.Sprintf("elevenlabs auth failed: status=%d body=%s", e.status, e.body)
}

// OpenSession acquires a WebSocket (pre-connected or fresh) and starts the
// reader goroutine.
func (et *elevenLabsTranscriber) OpenSession(ctx context.Context, deliver utils.DeliverFunc) (utils.ProviderSession, error) {
	ws, err := et.acquireWS(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire websocket: %w", err)
	}
	ws.SetReadLimit(1 << 20)
	es := &elevenLabsSession{
		et:          et,
		deliver:     deliver,
		ws:          ws,
		committedCh: make(chan string, 1),
	}
	es.startReader(ctx, ws) // creates es.done
	return es, nil
}

func (et *elevenLabsTranscriber) Close() error {
	et.bgCancel()
	et.mu.Lock()
	ch := et.prefetchCh
	et.prefetchCh = nil
	et.mu.Unlock()
	if ch != nil {
		// Drain and close the in-flight pre-connect so it isn't leaked.
		go func() {
			if res := <-ch; res.ws != nil {
				_ = res.ws.Close(websocket.StatusNormalClosure, "")
			}
		}()
	}
	return nil
}

// schedulePrefetch kicks off a background pre-connect for the next session, if
// one isn't already in flight. The socket is parked in prefetchCh until the next
// session claims it; ElevenLabs idle-closes it after ~15s, so a cold session
// (idle gap) detects the stale socket on its first chunk and reconnects.
func (et *elevenLabsTranscriber) schedulePrefetch() {
	et.mu.Lock()
	defer et.mu.Unlock()
	if et.prefetchCh != nil {
		return
	}
	ch := make(chan wsResult, 1)
	et.prefetchCh = ch
	go func() {
		ws, err := et.openWS(et.bgCtx)
		ch <- wsResult{ws: ws, err: err}
	}()
}

// acquireWS returns the pre-connected WebSocket if ready, otherwise opens a
// fresh one.
func (et *elevenLabsTranscriber) acquireWS(ctx context.Context) (*websocket.Conn, error) {
	et.mu.Lock()
	ch := et.prefetchCh
	et.prefetchCh = nil
	et.mu.Unlock()

	if ch != nil {
		select {
		case res := <-ch:
			if res.err == nil && res.ws != nil {
				return res.ws, nil
			}
			et.logger.Warnf("prefetch failed (%v); opening fresh connection", res.err)
		case <-time.After(10 * time.Second):
			et.logger.Warnf("prefetch timed out; opening fresh connection")
			// Don't leak the connection the pre-connect eventually produces.
			go func() {
				if res := <-ch; res.ws != nil {
					_ = res.ws.Close(websocket.StatusNormalClosure, "")
				}
			}()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// Fallback: blocking open (pre-connect unavailable).
	return et.openWS(ctx)
}

// openWS fetches a single-use token and opens a WebSocket, retrying once on any
// failure (the first attempt on startup is prone to transient errors that
// resolve immediately).
func (et *elevenLabsTranscriber) openWS(ctx context.Context) (*websocket.Conn, error) {
	wsURL := et.buildWSURL()
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		token, err := et.getSingleUseToken(ctx)
		if err != nil {
			lastErr = err
			et.logger.Warnf("prefetch attempt %d failed: %v", attempt+1, err)
			continue
		}
		dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		ws, _, err := websocket.Dial(dialCtx, wsURL+"&token="+token, nil)
		cancel()
		if err != nil {
			lastErr = err
			et.logger.Warnf("prefetch attempt %d ws dial failed: %v", attempt+1, err)
			continue
		}
		et.logger.Debugf("ws connected (attempt %d, model=%s lang=%s)", attempt+1, et.modelID, et.languageCode)
		return ws, nil
	}
	return nil, lastErr
}

// getSingleUseToken exchanges the API key for a short-lived realtime token. The
// realtime WebSocket endpoint requires this rather than the raw API key.
func (et *elevenLabsTranscriber) getSingleUseToken(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, elevenLabsTokenURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("xi-api-key", et.apiKey)
	resp, err := et.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return "", &elevenLabsAuthError{status: resp.StatusCode, body: string(body)}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("token fetch failed: status=%d body=%s", resp.StatusCode, string(body))
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("parse token response: %w", err)
	}
	if out.Token == "" {
		return "", fmt.Errorf("token response missing token field")
	}
	return out.Token, nil
}

func (et *elevenLabsTranscriber) buildWSURL() string {
	params := neturl.Values{}
	params.Set("model_id", et.modelID)
	params.Set("audio_format", fmt.Sprintf("pcm_%d", et.sampleRate))
	params.Set("commit_strategy", "manual")
	if et.languageCode != "" {
		params.Set("language_code", et.languageCode)
	}
	return elevenLabsWSBase + "?" + params.Encode()
}

// inputAudioChunk is the client → server message shape.
type inputAudioChunk struct {
	MessageType string `json:"message_type"`
	AudioBase64 string `json:"audio_base_64"`
	Commit      bool   `json:"commit"`
	SampleRate  int32  `json:"sample_rate"`
}

// serverMessage is the subset of the server → client message shape we use.
type serverMessage struct {
	MessageType string `json:"message_type"`
	Text        string `json:"text"`
	Error       string `json:"error"`
}

// elevenLabsSession is one ElevenLabs Scribe WebSocket session, scoped to a
// single audio segment.
type elevenLabsSession struct {
	et      *elevenLabsTranscriber
	deliver utils.DeliverFunc

	ws         *websocket.Conn
	chunksSent int

	readerCancel context.CancelFunc

	done        chan struct{} // current reader's death channel; closed by that reader on fatal error/close
	committedCh chan string   // post-commit committed transcript text

	mu                 sync.Mutex
	commitSent         bool   // set true once Finish sends commit:true
	earlyCommittedText string // server-VAD auto-commit before our commit
	partialCount       int    // number of partial_transcript messages
}

func (es *elevenLabsSession) startReader(ctx context.Context, ws *websocket.Conn) {
	// Each reader owns a fresh done channel. On a stale-WS reconnect the old
	// reader may have already closed its own done (it saw the server close that
	// triggered the reconnect); a new channel ensures that latched close can't
	// poison the reconnected session. es.done is only read/written on the
	// listener goroutine (startReader / Done), so no lock is needed.
	done := make(chan struct{})
	es.done = done
	rctx, cancel := context.WithCancel(ctx)
	es.readerCancel = cancel
	go es.readLoop(rctx, ws, done)
}

func (es *elevenLabsSession) stopReader() {
	if es.readerCancel != nil {
		es.readerCancel()
		es.readerCancel = nil
	}
}

// readLoop drains server messages until a fatal error/close, an auth error, or
// a post-commit committed transcript. On intentional stop (reconnect/close) the
// context is cancelled and it exits silently, leaving done open. On a fatal
// connection error it closes its own done channel.
func (es *elevenLabsSession) readLoop(ctx context.Context, ws *websocket.Conn, done chan struct{}) {
	for {
		_, data, err := ws.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // intentional stop; not a connection death
			}
			if websocket.CloseStatus(err) != -1 {
				es.et.logger.Warnf("server closed connection: %v", err)
			} else {
				es.et.logger.Warnf("reader error: %v", err)
			}
			close(done)
			return
		}
		var msg serverMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			es.et.logger.Warnf("server sent unparseable message: %s", string(data))
			continue
		}
		es.et.logger.Debugf("ws recv: message_type=%s", msg.MessageType)
		switch msg.MessageType {
		case "session_started":
			es.et.logger.Debug("session_started received")
		case "partial_transcript":
			es.mu.Lock()
			es.partialCount++
			es.mu.Unlock()
			es.et.logger.Debugf("ws recv partial: %q", msg.Text)
		case "auth_error":
			es.et.logger.Errorf("auth error: %s", msg.Error)
			close(done)
			return
		case "committed_transcript", "committed_transcript_with_timestamps":
			es.mu.Lock()
			commitSent := es.commitSent
			es.mu.Unlock()
			if commitSent {
				// Legitimate post-commit transcript.
				es.et.logger.Infof("ws recv committed_transcript: %q", msg.Text)
				select {
				case es.committedCh <- msg.Text:
				default:
				}
				return
			}
			// Server VAD fired before our manual commit; hold the text as a
			// fallback but keep the reader alive for the post-commit transcript.
			es.et.logger.Warnf("server auto-committed before manual commit (VAD); holding early text: %q", msg.Text)
			es.mu.Lock()
			es.earlyCommittedText = msg.Text
			es.mu.Unlock()
		default:
			// An unrecognized message_type would otherwise be silently dropped
			// and surface only as a 30s commit timeout. Surface it so a protocol
			// change (or a server-side error event) is immediately visible.
			es.et.logger.Warnf("ws recv: unhandled message_type=%q raw=%s", msg.MessageType, truncateMsg(string(data)))
		}
	}
}

// truncateMsg bounds a raw server payload for logging — committed_transcript
// _with_timestamps responses can be large.
func truncateMsg(s string) string {
	const max = 300
	if len(s) > max {
		return s[:max] + "...(truncated)"
	}
	return s
}

func (es *elevenLabsSession) SendAudio(ctx context.Context, pcm []byte) error {
	msg, err := json.Marshal(inputAudioChunk{
		MessageType: "input_audio_chunk",
		AudioBase64: base64.StdEncoding.EncodeToString(pcm),
		Commit:      false,
		SampleRate:  es.et.sampleRate,
	})
	if err != nil {
		return err
	}
	es.et.logger.Debugf("ws send: chunk #%d (%d bytes)", es.chunksSent, len(pcm))

	if es.chunksSent == 0 {
		// First chunk: a pre-connected WS may be stale (the server closes idle
		// connections after ~15s). If the write fails, reconnect once and retry
		// the chunk transparently.
		if werr := es.ws.Write(ctx, websocket.MessageText, msg); werr != nil {
			es.et.logger.Warnf("pre-connected WS stale; reconnecting: %v", werr)
			es.stopReader()
			_ = es.ws.Close(websocket.StatusNormalClosure, "")
			ws, aerr := es.et.acquireWS(ctx)
			if aerr != nil {
				return aerr
			}
			ws.SetReadLimit(1 << 20)
			es.ws = ws
			es.startReader(ctx, ws)
			if werr2 := es.ws.Write(ctx, websocket.MessageText, msg); werr2 != nil {
				return werr2
			}
		}
	} else if werr := es.ws.Write(ctx, websocket.MessageText, msg); werr != nil {
		return werr
	}
	es.chunksSent++
	return nil
}

func (es *elevenLabsSession) Done() <-chan struct{} {
	return es.done
}

// Finish sends the commit, kicks off the next pre-connect, waits for the
// committed transcript, delivers it, and returns the transcription-derived
// session fields.
func (es *elevenLabsSession) Finish(ctx context.Context, reason string) (utils.SessionReading, error) {
	recvStart := time.Now()

	es.mu.Lock()
	es.commitSent = true
	es.mu.Unlock()

	commitMsg, _ := json.Marshal(inputAudioChunk{
		MessageType: "input_audio_chunk",
		AudioBase64: "",
		Commit:      true,
		SampleRate:  es.et.sampleRate,
	})
	es.et.logger.Debugf("ws send: commit (chunks_sent=%d)", es.chunksSent)
	if err := es.ws.Write(ctx, websocket.MessageText, commitMsg); err != nil {
		es.et.logger.Warnf("commit send failed: %v", err)
	}

	// Pre-connect for the next session while we wait for this transcript.
	es.et.schedulePrefetch()

	var text string
	timedOut := false
	select {
	case text = <-es.committedCh:
	case <-es.done:
	case <-time.After(30 * time.Second):
		timedOut = true
		es.et.logger.Warnf("timeout waiting for committed_transcript")
	case <-ctx.Done():
	}

	recvUs := time.Since(recvStart).Microseconds()
	es.et.logger.Debugf("elevenlabs response latency: %dms (commit→committed)", recvUs/1000)

	es.mu.Lock()
	if text == "" {
		text = es.earlyCommittedText // server-VAD fallback
	}
	partialCount := es.partialCount
	es.mu.Unlock()

	es.stopReader()
	_ = es.ws.Close(websocket.StatusNormalClosure, "")

	if text != "" {
		es.deliver(text, 0.0)
	}

	closeReason := "success"
	if text == "" {
		switch {
		case reason == "audio_in channel closed":
			closeReason = "context_cancelled"
		case reason == "send error" || reason == "recv died":
			closeReason = "ws_error"
		case timedOut:
			closeReason = "timeout"
		default:
			closeReason = "no_result"
		}
	}

	finalCount := 0
	if text != "" {
		finalCount = 1
	}
	es.et.logger.Debugf("session resolved: close_reason=%s partials=%d transcript=%q", closeReason, partialCount, text)

	return utils.SessionReading{
		Transcript:    text,
		Confidence:    0.0,
		CloseReason:   closeReason,
		LanguageCode:  es.et.languageCode,
		Model:         es.et.modelID,
		ResponseCount: partialCount + finalCount,
		FinalCount:    finalCount,
		InterimCount:  partialCount,
	}, nil
}

func (es *elevenLabsSession) Close() {
	es.stopReader()
	if es.ws != nil {
		_ = es.ws.Close(websocket.StatusNormalClosure, "")
	}
}

// ensure elevenLabsSession satisfies ProviderSession and elevenLabsTranscriber
// satisfies Transcriber at compile time.
var (
	_ utils.ProviderSession = (*elevenLabsSession)(nil)
	_ utils.Transcriber     = (*elevenLabsTranscriber)(nil)
)
