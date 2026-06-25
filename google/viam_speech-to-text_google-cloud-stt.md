# Model viam:speech-to-text:google-cloud-stt

A Viam generic component that performs streaming speech-to-text using
[Google Cloud Speech-to-Text v2](https://cloud.google.com/speech-to-text/v2/docs).

The module reads PCM16 audio chunks from a configured `audio_in` dependency,
streams them to Google Cloud, and pushes final transcripts into a configured
`transcript_target` generic resource via `DoCommand`. If no `transcript_target`
is configured, finals are logged to stdout (useful for local testing).

**Must be used with a `filter-mic` instance.**

### Wire shape

1. Module starts and spawns a background listener on the `mic` source.
2. When `mic` emits audio chunks (e.g. its wake word fired), the module opens
   a Google streaming session and forwards chunks.
3. When Google returns a final transcript, the module calls
   `transcript_target.DoCommand(...)` with the payload below.
4. When `mic` emits its segment-end sentinel (an empty `AudioChunk`), the
   module closes the Google session and returns to waiting.

The transcript callback payload:

```json
{
  "command": "deliverTranscript",
  "transcript": "turn on the lights",
  "is_final": true,
  "confidence": 0.92,
  "source": "google-cloud-stt"
}
```

## Configuration

The following attribute template can be used to configure this model:

```json
{
  "mic": "<audio_in component name>",
  "transcript_target": "<generic resource name>",
  "google_credentials_json": { },
  "language_code": "en-US",
  "model": "latest_short",
  "location": "global",
  "sample_rate_hertz": 16000,
  "max_session_seconds": 290,
  "session_sensor_name": "<session-sensor component name>"
}
```

### Attributes

| Name                      | Type    | Inclusion | Description                                                                                                                                       |
|---------------------------|---------|-----------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| `mic`                     | string  | Required  | Resource name of an `audio_in` component that emits PCM16 16kHz mono audio (e.g. a `viam-wake-filter` instance).                                  |
| `transcript_target`       | string  | Optional  | Resource name of a generic resource to receive `deliverTranscript` DoCommand calls. If empty, the module runs in log-only mode.                   |
| `google_credentials_json` | object  | Required  | Inline GCP service-account JSON. The `project_id` field is read from this to build the v2 recognizer path.                                        |
| `language_code`           | string  | Optional  | Recognition language. Defaults to `en-US`.                                                                                                        |
| `model`                   | string  | Optional  | Recognition model. v2 supports `latest_short` (default, for commands), `latest_long`, `chirp_2`, `chirp_3`, etc.                                  |
| `location`                | string  | Optional  | Google Cloud region for the Speech v2 recognizer. Defaults to `global`. Regional models like `chirp_2` require a regional endpoint (e.g. `us-central1`). |
| `sample_rate_hertz`       | int     | Optional  | Sample rate of the input audio. Defaults to `16000`.                                                                                              |
| `max_session_seconds`     | int     | Optional  | Caps a single Google streaming session. Google's hard cap is 305s; defaults to `290`.                                                             |
| `session_sensor_name`     | string  | Optional  | Resource name of a `viam:speech-to-text:session-sensor` to capture per-session audio + metadata to the Viam Data tab. If empty, capture is disabled. See [the session-sensor model docs](../utils/viam_speech-to-text_session-sensor.md). |

### Example Configuration

```json
{
  "mic": "filter-mic",
  "transcript_target": "voice-router",
  "language_code": "en-US",
  "model": "latest_short",
  "google_credentials_json": {
    "type": "service_account",
    "project_id": "my-gcp-project",
    "private_key_id": "...",
    "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
    "client_email": "stt-sa@my-gcp-project.iam.gserviceaccount.com",
    "client_id": "...",
    "token_uri": "https://oauth2.googleapis.com/token"
  }
}
```

## DoCommand

The module's primary output flows to `transcript_target` — there is no need
for consumers to poll it. `DoCommand` exposes a single command for debugging.

### `status`

Returns the configured target and whether the module is in log-only mode.

```json
{
  "command": "status"
}
```

Response:

```json
{
  "transcript_target": "voice-router",
  "log_only": false
}
```
