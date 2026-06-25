# Module speech-to-text

Voice-pipeline components for Viam: streaming speech-to-text generic components
(Google Cloud STT and ElevenLabs Scribe), plus a session-sensor that captures
per-session WAV + metadata into the Viam Data tab for debugging and dataset
building.

## Models

- [`viam:speech-to-text:google-cloud-stt`](google/viam_speech-to-text_google-cloud-stt.md) — Streams audio from an `audio_in` source to Google Cloud Speech-to-Text v2 and dispatches final transcripts.
- [`viam:speech-to-text:elevenlabs-stt`](elevenlabs/viam_speech-to-text_elevenlabs-stt.md) — Streams audio from an `audio_in` source to the ElevenLabs Scribe realtime WebSocket API and dispatches final transcripts.
- [`viam:speech-to-text:session-sensor`](utils/viam_speech-to-text_session-sensor.md) — Sensor that captures per-session audio + metadata for each STT session and routes it to the Viam Data tab (binary WAV + tabular row, joined by `binary_data_id`).

The two STT models share the same `audio_in` contract, transcript dispatch
(`deliverTranscript` to a `transcript_target`), and session-sensor capture. They
differ only in the cloud backend and its credentials/config, so a deployment can
switch providers by swapping which model it instantiates and pointing both at
the same session-sensor.
