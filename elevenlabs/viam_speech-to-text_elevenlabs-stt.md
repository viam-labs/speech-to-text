# Model viam:speech-to-text:elevenlabs-stt

A Viam generic component that performs speech-to-text using the
[ElevenLabs Scribe](https://elevenlabs.io/docs) realtime WebSocket API
(`wss://api.elevenlabs.io/v1/speech-to-text/realtime`).

The module reads PCM16 audio chunks from a configured `audio_in` dependency,
streams them to ElevenLabs Scribe, and pushes the final transcript into a
configured `transcript_target` generic resource via `DoCommand`. If no
`transcript_target` is configured, finals are logged (useful for local testing).

This model shares the `audio_in` contract, transcript dispatch, and
session-sensor capture with [`google-cloud-stt`](../google/viam_speech-to-text_google-cloud-stt.md);
it differs only in the cloud backend and its config.

> **Supported model: Scribe v2 realtime only.** This module is built entirely
> around ElevenLabs' Scribe realtime streaming protocol (it streams audio chunks
> over a WebSocket and commits at end of speech). The model is therefore fixed to
> `scribe_v2_realtime` and is **not** configurable — ElevenLabs' non-streaming /
> batch speech-to-text models use a different API the module does not implement.

**Must be used with a `filter-mic` instance** — an `audio_in` source that gates
audio on a wake word and emits a segment-end sentinel (an empty `AudioChunk`).

### Wire shape

1. Module starts, spawns a background listener on the `mic` source, and
   pre-connects a WebSocket for the first session.
2. When `mic` emits audio chunks (e.g. its wake word fired), the module
   acquires the pre-connected WebSocket (or opens a fresh one) and forwards each
   PCM16 chunk base64-encoded.
3. When `mic` emits its segment-end sentinel, the module sends a commit, waits
   for the `committed_transcript` response, and calls
   `transcript_target.DoCommand(...)` with the payload below.
4. The WebSocket is closed after each committed transcript; the next session's
   WebSocket is pre-connected in the background.

The transcript callback payload (note `confidence` is always `0.0` — Scribe
realtime does not return a per-transcript confidence):

```json
{
  "command": "deliverTranscript",
  "transcript": "turn on the lights",
  "is_final": true,
  "confidence": 0.0,
  "source": "elevenlabs-stt"
}
```

> **Connection latency.** ElevenLabs idle-closes realtime sockets after ~15s, so
> a session that follows an idle gap reconnects on its first chunk (a few hundred
> ms). This happens while the user is still speaking, so it overlaps the audio
> and stays out of the post-utterance response latency. Rapid follow-up sessions
> reuse the pre-connected socket.

## Configuration

The following attribute template can be used to configure this model:

```json
{
  "mic": "<audio_in component name>",
  "transcript_target": "<generic resource name>",
  "api_key": "<ElevenLabs API key>",
  "language_code": "en",
  "sample_rate_hertz": 16000,
  "session_sensor_name": "<session-sensor component name>"
}
```

### Attributes

| Name                  | Type   | Inclusion | Description                                                                                                                                       |
|-----------------------|--------|-----------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| `mic`                 | string | Required  | Resource name of an `audio_in` component that emits PCM16 mono audio (e.g. a `viam-wake-filter` instance).                                        |
| `api_key`             | string | Required  | ElevenLabs API key. Validated at construction with a single token fetch — a rejected key (HTTP 401/403) fails the resource; transient errors are tolerated and retried at runtime. |
| `transcript_target`   | string | Optional  | Resource name of a generic resource to receive `deliverTranscript` DoCommand calls. If empty, the module runs in log-only mode.                   |
| `language_code`       | string | Optional  | Recognition language. Defaults to `en`.                                                                                                           |
| `sample_rate_hertz`   | int    | Optional  | Sample rate of the input audio. Defaults to `16000`.                                                                                              |
| `session_sensor_name` | string | Optional  | Resource name of a `viam:speech-to-text:session-sensor` to capture per-session audio + metadata to the Viam Data tab. If empty, capture is disabled. See [the session-sensor model docs](../utils/viam_speech-to-text_session-sensor.md). |

### Example Configuration

```json
{
  "mic": "filter-mic",
  "transcript_target": "voice-router",
  "api_key": "sk_...",
  "language_code": "en"
}
```

## DoCommand

The module's primary output flows to `transcript_target` — there is no need for
consumers to poll it. `DoCommand` exposes a single command for debugging.

### `status`

Returns the configured target, whether the module is in log-only mode, and the
active model/language.

```json
{
  "command": "status"
}
```

Response:

```json
{
  "transcript_target": "voice-router",
  "log_only": false,
  "model_id": "scribe_v2_realtime",
  "language_code": "en"
}
```
