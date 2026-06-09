# Module speech-to-text

Voice-pipeline components for Viam: a streaming Google Cloud STT generic
component, plus a session-sensor that captures per-session WAV + metadata
into the Viam Data tab for debugging and dataset building.

## Models

- [`viam:speech-to-text:google-cloud-stt`](viam_speech-to-text_google-cloud-stt.md) — Streams audio from an `audio_in` source to Google Cloud Speech-to-Text v2 and dispatches final transcripts.
- [`viam:speech-to-text:session-sensor`](viam_speech-to-text_session-sensor.md) — Sensor that captures per-session audio + metadata for each STT session and routes it to the Viam Data tab (binary WAV + tabular row, joined by `binary_data_id`).
