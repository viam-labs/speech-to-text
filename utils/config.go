package utils

// STTConfig holds the machine.json attributes common to every STT model in this
// module. Provider configs embed it and add only their provider-specific
// fields.
//
// Embed it with the `json:",squash"` tag so the fields stay flat in the config
// JSON and decode correctly through Viam's mapstructure attribute decoder
// (which keys off the `json` tag and does not squash embedded structs by
// default):
//
//	type Config struct {
//		utils.STTConfig `json:",squash"`
//		ProviderField   string `json:"provider_field,omitempty"`
//	}
type STTConfig struct {
	// Mic is the Viam resource name of an audio_in component that emits PCM16
	// mono audio (e.g. a viam-wake-filter instance). Required.
	Mic string `json:"mic"`

	// TranscriptTarget is the Viam resource name of a generic component the
	// module calls with each final transcript via deliverTranscript. If empty,
	// the model runs in log-only mode.
	TranscriptTarget string `json:"transcript_target,omitempty"`

	// LanguageCode for recognition. The default is provider-specific and applied
	// by each model's constructor.
	LanguageCode string `json:"language_code,omitempty"`

	// SampleRateHertz of the input audio. Defaults to 16000.
	SampleRateHertz int32 `json:"sample_rate_hertz,omitempty"`

	// SessionSensorName is the Viam resource name of a
	// viam:speech-to-text:session-sensor to capture per-session WAV + metadata
	// to the Data tab. Optional — empty disables capture.
	SessionSensorName string `json:"session_sensor_name,omitempty"`
}
