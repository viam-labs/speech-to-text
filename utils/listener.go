// Package utils holds the provider-agnostic core shared by the STT models:
// the listener (reading audio from an audio_in source, detecting segment
// boundaries, dispatching final transcripts to a transcript_target, and
// capturing per-session WAV + metadata), the Transcriber/ProviderSession
// backend interfaces, and the session-sensor. Each STT backend (Google,
// ElevenLabs) lives in its own package and plugs in as a Transcriber.
//
// Wire shape (inverted callback):
//
//  1. Module starts. The listener spawns a background goroutine on the audio_in
//     source.
//  2. When audio_in emits audio chunks (e.g. a wake filter fired upstream), the
//     listener opens a provider session and forwards chunks.
//  3. As the provider produces a final transcript, the listener calls
//     transcript_target.DoCommand({
//     "command": "deliverTranscript",
//     "transcript": "...",
//     "is_final": true,
//     "confidence": 0.92,
//     "source": "<provider>",
//     })
//  4. When audio_in emits its segment-end sentinel (empty AudioChunk), the
//     listener finishes the provider session, captures it, and goes back to
//     waiting. A provider may also end the session itself before the sentinel
//     (e.g. Google VoiceActivityTimeout), signalled via ProviderSession.Done().
//
// If transcript_target is unset (testing mode), finals are logged but not
// dispatched. If session_sensor_name is unset, session capture is skipped.
package utils

import (
	"bytes"
	"context"
	"errors"
	"io"
	"time"

	"github.com/google/uuid"

	audioin "go.viam.com/rdk/components/audioin"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
	rutils "go.viam.com/rdk/utils"
	goutils "go.viam.com/utils"
)

// Transcriber is a provider-specific STT backend. The shared listener owns the
// mic, segment boundaries, transcript dispatch, and session capture; a
// Transcriber turns one audio segment into transcripts plus per-session metrics.
type Transcriber interface {
	// OpenSession starts a provider session for one audio segment. deliver is
	// invoked for each final transcript as it becomes available — Google may
	// call it multiple times mid-stream; ElevenLabs calls it once at commit.
	OpenSession(ctx context.Context, deliver DeliverFunc) (ProviderSession, error)
	// Close releases any provider-level resources (e.g. the gRPC client).
	Close() error
}

// DeliverFunc dispatches one final transcript to the transcript_target.
type DeliverFunc func(text string, confidence float64)

// ProviderSession is one provider streaming session, scoped to a single audio
// segment.
type ProviderSession interface {
	// SendAudio forwards one non-empty PCM16 chunk to the provider. A returned
	// io.EOF means the provider closed the stream first (not an error); any
	// other error is a genuine send failure.
	SendAudio(ctx context.Context, pcm []byte) error
	// Done is closed when the provider ends the session itself (e.g. Google
	// VoiceActivityTimeout fires after emitting a final). The listener then
	// finishes the session. For providers that only finish on the sentinel
	// (ElevenLabs), this channel effectively never fires before Finish.
	Done() <-chan struct{}
	// Finish signals segment end, waits for the final result, and returns the
	// transcription-derived fields of the session reading (transcript,
	// confidence, counts, normalized close_reason, error_message, language,
	// model). The listener fills the remaining fields (capture_id, audio bytes,
	// WAV, timestamps). reason is the raw close reason the listener observed.
	Finish(ctx context.Context, reason string) (SessionReading, error)
	// Close tears the session down without waiting, for cancellation paths.
	Close()
}

// Listener is the shared background loop that connects an audio_in source to a
// Transcriber and routes transcripts + session captures downstream. Construct
// one with the fields below populated, then call Start.
type Listener struct {
	Logger logging.Logger

	AudioIn          audioin.AudioIn
	TranscriptTarget resource.Resource // generic resource; nil in log-only mode
	TargetName       string
	SessionSink      SessionSensorSink // nil if session capture disabled
	SampleRate       int32
	Source           string // value of the "source" field in deliverTranscript
	Transcriber      Transcriber

	workers *goutils.StoppableWorkers
}

// Start spawns the background listener goroutine.
func (l *Listener) Start() {
	l.workers = goutils.NewBackgroundStoppableWorkers(l.runListener)
}

// Stop halts the background listener goroutine.
func (l *Listener) Stop() {
	if l.workers != nil {
		l.workers.Stop()
	}
}

// runListener reads from the audio_in source for the lifetime of the module.
// When chunks start arriving (wake fired upstream), drainAudio opens a provider
// session and forwards them; when the segment-end sentinel arrives, it finishes
// the session and loops back to waiting.
func (l *Listener) runListener(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		audioChan, err := l.AudioIn.GetAudio(ctx, "pcm16", 0, 0, nil)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			l.Logger.Warnf("audio_in.GetAudio: %v; retrying in 1s", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		l.drainAudio(ctx, audioChan)
	}
}

// drainAudio reads chunks from a single audio_in channel until it closes or the
// module shuts down, managing the lifecycle of one provider session per audio
// segment. The PCM buffer, audio-sent count, capture id, and session timing are
// owned here (provider-agnostic); transcription is delegated to the session.
func (l *Listener) drainAudio(ctx context.Context, audioChan <-chan *audioin.AudioChunk) {
	var session ProviderSession
	var benchStart time.Time
	var captureID string
	var startedAt time.Time
	var audioBuf bytes.Buffer
	var audioSent int
	var sessionOpenUs int64

	// deliver dispatches a final transcript. Google calls it from its recv
	// goroutine; ElevenLabs calls it inside Finish on the same goroutine.
	deliver := func(text string, confidence float64) {
		l.deliverFinal(ctx, text, confidence)
	}

	// finish closes the active session, stamps the listener-owned fields onto
	// the reading the provider returns, and pushes the capture. Caller must set
	// session = nil afterward.
	finish := func(reason string) {
		reading, _ := session.Finish(ctx, reason)

		reading.CaptureID = captureID
		reading.AudioSentBytes = audioSent
		reading.StartTime = startedAt
		reading.EndTimeUs = time.Now().UnixMicro()
		reading.SessionOpenUs = sessionOpenUs
		wav, err := audioin.CreateWAVFile(audioBuf.Bytes(), l.SampleRate, 1, rutils.CodecPCM16)
		if err != nil {
			l.Logger.Errorf("session-sensor: wrap PCM as WAV (capture_id=%s): %v", captureID, err)
			wav = nil
		}
		reading.WAV = wav

		if reading.CloseReason != "success" {
			l.Logger.Infof("transcription unsuccessful (%s)", reading.CloseReason)
		}
		l.Logger.Debugf("session closed (%s) audio_sent=%d bytes", reading.CloseReason, audioSent)
		l.pushSessionCapture(reading)
	}

	defer func() {
		if session != nil {
			finish("audio_in channel closed")
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
					finish("segment-end sentinel")
					session = nil
				}
				continue
			}
			if session != nil {
				// Non-blocking check: did the provider end the session itself?
				select {
				case <-session.Done():
					finish("recv died")
					session = nil
				default:
				}
			}
			if session == nil {
				benchStart = time.Now()
				captureID = uuid.NewString()
				startedAt = benchStart
				audioBuf.Reset()
				audioSent = 0
				opened, err := l.Transcriber.OpenSession(ctx, deliver)
				sessionOpenUs = time.Since(benchStart).Microseconds()
				if err != nil {
					l.Logger.Warnf("%v", err)
					continue
				}
				l.Logger.Debugf("session opened (first audio chunk) capture_id=%s", captureID)
				session = opened
			}
			if err := session.SendAudio(ctx, chunk.AudioData); err != nil {
				// io.EOF means the provider closed the stream before we finished
				// sending — the expected race when Google's VoiceActivityTimeout
				// fires. Not an error; normalizeCloseReason maps "recv died" +
				// finals to success. Any other error is a genuine send failure.
				reason := "send error"
				if errors.Is(err, io.EOF) {
					reason = "recv died"
				} else {
					l.Logger.Warnf("send audio: %v", err)
				}
				finish(reason)
				session = nil
				continue
			}
			audioSent += len(chunk.AudioData)
			audioBuf.Write(chunk.AudioData)
		}
	}
}

// deliverFinal dispatches one final transcript to transcript_target (or logs,
// in log-only mode).
func (l *Listener) deliverFinal(ctx context.Context, text string, confidence float64) {
	l.Logger.Infof("transcript: %q (confidence=%.2f)", text, confidence)
	if l.TranscriptTarget == nil {
		return
	}
	doCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := l.TranscriptTarget.DoCommand(doCtx, map[string]interface{}{
		"command":    "deliverTranscript",
		"transcript": text,
		"is_final":   true,
		"confidence": confidence,
		"source":     l.Source,
	})
	if err != nil {
		l.Logger.Warnf("deliverTranscript to %s: %v", l.TargetName, err)
	}
}

// pushSessionCapture forwards a completed session reading to the configured
// session-sensor sink in a goroutine so a slow upload never blocks the audio
// loop. No-op if no sink is configured.
func (l *Listener) pushSessionCapture(r SessionReading) {
	if l.SessionSink == nil {
		l.Logger.Debugf("session-sensor not configured — skipping push for close_reason=%s", r.CloseReason)
		return
	}
	if r.CaptureID == "" {
		return
	}
	sink := l.SessionSink
	logger := l.Logger
	go func() {
		// Detach from the caller's ctx — the audio loop may be shutting down and
		// we don't want to abort the push.
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
