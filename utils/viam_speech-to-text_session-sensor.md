# Model viam:speech-to-text:session-sensor

A Viam sensor component that captures one tabular reading per completed STT
session and uploads the corresponding WAV to the Viam binary store. The audio
and the metadata are linked by `binary_data_id` so they can be joined in MQL.

Use it to:

- Listen to the exact audio when STT misbehaves (no-result, error close paths).
- Build a labelled dataset of voice sessions for model evaluation.
- Distinguish wake/STT/cloud/network failure modes offline.

The sensor accepts pushes two ways:

- **In-process Go interface** (`SessionSensorSink`) — STT models in this repo
  (`google-cloud-stt`, `elevenlabs-stt`) call `PushSession(ctx, SessionReading)`
  directly. Zero gRPC overhead.
- **`DoCommand({"command": "push_session", ...})`** — STT modules in other
  repos push over gRPC with `audio_wav_b64` in the payload.

Both paths converge on the same internal queue + binary upload.


## Configuration

```json
{
}
```

### Attributes

| Name             | Type     | Inclusion | Description                                                                                                                |
|------------------|----------|-----------|----------------------------------------------------------------------------------------------------------------------------|
| `dataset_ids`    | string[] | Optional  | Dataset IDs attached to each binary upload — routes captured WAVs into a named training/debug dataset in the Viam app. All entries must be non-empty strings. |
| `max_queue_size` | int      | Optional  | Soft cap on pending readings. When exceeded, the oldest reading is dropped with a warning. Must be `>= 0`; `0` (or unset) applies the default of `1000`. |

### Environment variables

The sensor reads `VIAM_API_KEY`, `VIAM_API_KEY_ID`, and `VIAM_MACHINE_PART_ID`
to construct a Viam app client for binary uploads. The Viam module manager
sets these automatically. If they're missing (e.g. local dev), the sensor
still runs in "tabular-only" mode — readings are queued with empty
`binary_data_id`.

### Data manager configuration

The sensor implements `Readings()` and is meant to be polled by the Viam
**data manager** at a configurable cadence (1 Hz is fine — the sensor returns
one queued reading per poll, or `data.ErrNoCaptureToStore` when the queue is
empty so the data manager doesn't write blank rows).

Make sure the data manager has **both capture and sync enabled** on this
sensor — capture writes rows to disk locally, sync uploads them to the Data
tab.

## DoCommand

### `push_session`

Cross-process push entry point for STT modules in other repos. Same code path
as the in-process Go sink.

Required fields:

```json
{
  "command": "push_session",
  "capture_id": "<uuid>",
  "close_reason": "success",
  "start_time": "<RFC3339Nano>",
  "end_time_us": 1782156234078695
}
```

Optional fields (all default to empty / zero):

| Field             | Type   | Notes                                                                                                |
|-------------------|--------|------------------------------------------------------------------------------------------------------|
| `audio_wav_b64`     | string | Base64-encoded WAV bytes (16 kHz mono PCM16). Transport-only — uploaded to binary store, not stored. |
| `transcript`        | string | Final transcript. Empty for `no_result`/error close paths.                                           |
| `confidence`        | float  | `0.0` ≤ x ≤ `1.0`. Google v2 documents this as unreliable / not always populated; ElevenLabs always sends `0.0`. |
| `error_message`     | string | Underlying error for error close paths (e.g. the gRPC error from Google).                            |
| `audio_sent_bytes`  | int    | Bytes forwarded to the STT backend.                                                                  |
| `language_code`     | string | e.g. `"en-US"` (Google) or `"en"` (ElevenLabs).                                                      |
| `model`             | string | STT model identifier.                                                                                |
| `response_count`    | int    | Total backend responses received during the session.                                                |
| `final_count`       | int    | Number of finals received.                                                                           |
| `interim_count`     | int    | Number of interim/partial results received.                                                          |

Response:

```json
{
  "capture_id": "<uuid>",
  "binary_data_id": "<id>"
}
```

## Tabular reading shape

What lands in Viam cloud per session (the queued reading returned by
`Readings()`):

| Field              | Type             | Notes                                                                                          |
|--------------------|------------------|------------------------------------------------------------------------------------------------|
| `capture_id`       | string           | UUID generated at session open by the STT module.                                              |
| `binary_data_id`   | string           | Returned by `BinaryDataCaptureUpload`. Empty if upload failed.                                 |
| `transcript`       | string           | Final transcript. Empty for no-result/error.                                                   |
| `confidence`         | float            | `0` = not set (Google v2 sentinel; ElevenLabs always `0`).                                     |
| `close_reason`       | string           | `success`, `no_result`, `context_cancelled`; plus `send_error`/`recv_error`/`timeout` (Google) and `ws_error`/`timeout` (ElevenLabs). |
| `error_message`      | string           | Underlying error for error close paths; empty otherwise.                                       |
| `audio_sent_bytes`   | int              | Bytes forwarded to the STT backend during the session.                                         |
| `start_time`         | RFC3339Nano      | Session-open timestamp. Also stamped on the binary record as `time_requested`.                 |
| `end_time_us`        | int              | Session-close time, Unix microseconds. Also stamped on the binary record as `time_received`.   |
| `duration_ms`        | float            | `end_time_us - start_time` in ms (microsecond precision).                                       |
| `language_code`      | string           | e.g. `"en-US"` (Google) or `"en"` (ElevenLabs).                                                |
| `model`              | string           | STT model identifier.                                                                          |
| `response_count`     | int              | Total backend responses received during the session.                                           |
| `final_count`        | int              | Number of finals received.                                                                      |
| `interim_count`      | int              | Number of interim/partial results received.                                                     |
| `captured_at`        | RFC3339Nano      | When the sensor appended the reading (server-side).                                            |

## Binary record ↔ tabular row linking

The binary blob (WAV) and the tabular reading land in **separate Viam stores**
and link two ways:

- **By `binary_data_id`** — the tabular row carries it; the binary record is
  keyed by it.
- **By `capture_id` tag** — the tabular row carries `capture_id` directly; the
  binary record is tagged with `capture_<capture_id>`. Useful for
  human-readable filtering in the Data tab.

### Binary upload tags

Every uploaded WAV is tagged with:

- `voice_session` — coarse "all voice captures" filter
- `capture_<capture_id>` — exact lookup of one session's WAV
- `success` / `no_result` / `error` — coarse close-reason bucket

### Example MQL: find the row for a given capture

```javascript
[
  { "$match": { "data.readings.capture_id": "<the capture uuid>" } }
]
```

### Example MQL: join row + binary by binary_data_id

```javascript
[
  { "$match": { "data.readings.close_reason": "recv_error" } },
  { "$lookup": {
      "from": "binary",
      "localField": "data.readings.binary_data_id",
      "foreignField": "_id",
      "as": "wav"
  } }
]
```

(Exact field names for the binary join may vary by Viam version — adjust
`foreignField` if `_id` doesn't match.)

## Example: wired with an STT model

Shown with `google-cloud-stt`; `elevenlabs-stt` wires up identically — set its
`session_sensor_name` to the same sensor (swap the STT model and its
credentials).

```json
{
  "components": [
    {
      "name": "stt",
      "model": "viam:speech-to-text:google-cloud-stt",
      "type": "generic",
      "attributes": {
        "mic": "filter-mic",
        "google_credentials_json": { },
        "session_sensor_name": "voice_session_sensor"
      }
    },
    {
      "name": "voice_session_sensor",
      "model": "viam:speech-to-text:session-sensor",
      "type": "sensor",
      "attributes": {
        "dataset_ids": ["my-voice-dataset"]
      }
    }
  ],
  "services": [
    {
      "name": "data_manager-1",
      "type": "data_manager",
      "attributes": {
        "capture_disabled": false,
        "sync_disabled": false,
        "capture_methods": [
          {
            "name": "voice_session_sensor",
            "method": "Readings",
            "capture_frequency_hz": 1
          }
        ]
      }
    }
  ]
}
```
