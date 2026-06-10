package speechtotext

import (
	"context"
	"errors"
	"io"
	"testing"

	speechpb "cloud.google.com/go/speech/apiv2/speechpb"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
)

func alt(text string, conf float32) *speechpb.SpeechRecognitionAlternative {
	return &speechpb.SpeechRecognitionAlternative{Transcript: text, Confidence: conf}
}

func TestFirstNonEmptyAlternative(t *testing.T) {
	for _, tc := range []struct {
		name     string
		alts     []*speechpb.SpeechRecognitionAlternative
		wantText string
		wantConf float32
	}{
		{"no alternatives", nil, "", 0},
		{"single empty", []*speechpb.SpeechRecognitionAlternative{alt("", 0)}, "", 0},
		{"whitespace only", []*speechpb.SpeechRecognitionAlternative{alt("   ", 0.5)}, "", 0},
		{"single non-empty", []*speechpb.SpeechRecognitionAlternative{alt("hello", 0.9)}, "hello", 0.9},
		{"empty first, non-empty second", []*speechpb.SpeechRecognitionAlternative{alt("", 0), alt("turn on the light", 0.8)}, "turn on the light", 0.8},
		{"all empty", []*speechpb.SpeechRecognitionAlternative{alt("", 0), alt(" ", 0)}, "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			text, conf := firstNonEmptyAlternative(tc.alts)
			if text != tc.wantText || conf != tc.wantConf {
				t.Errorf("got (%q, %v), want (%q, %v)", text, conf, tc.wantText, tc.wantConf)
			}
		})
	}
}

// fakeStream stubs the Google streaming client: Recv returns the scripted
// responses, then io.EOF. Only Recv is called by receiveFromGoogle.
type fakeStream struct {
	speechpb.Speech_StreamingRecognizeClient
	resps []*speechpb.StreamingRecognizeResponse
	i     int
}

func (f *fakeStream) Recv() (*speechpb.StreamingRecognizeResponse, error) {
	if f.i >= len(f.resps) {
		return nil, io.EOF
	}
	r := f.resps[f.i]
	f.i++
	return r, nil
}

// fakeTarget records deliverTranscript calls. Only DoCommand is called.
type fakeTarget struct {
	resource.Resource
	delivered []string
}

func (f *fakeTarget) DoCommand(_ context.Context, cmd map[string]interface{}) (map[string]interface{}, error) {
	f.delivered = append(f.delivered, cmd["transcript"].(string))
	return nil, nil
}

func finalResult(alts ...*speechpb.SpeechRecognitionAlternative) *speechpb.StreamingRecognizeResponse {
	return &speechpb.StreamingRecognizeResponse{
		Results: []*speechpb.StreamingRecognitionResult{{IsFinal: true, Alternatives: alts}},
	}
}

func TestReceiveFromGoogleEmptyFinals(t *testing.T) {
	for _, tc := range []struct {
		name          string
		resps         []*speechpb.StreamingRecognizeResponse
		wantFinals    int
		wantDelivered []string
		wantReason    string
	}{
		{
			// The buzzy-machine incident: one final whose only hypothesis is
			// empty. Must not count as success or reach the target.
			name:       "empty final only",
			resps:      []*speechpb.StreamingRecognizeResponse{finalResult(alt("", 0))},
			wantFinals: 0,
			wantReason: "no_result",
		},
		{
			// Chirp quirk: empty hypothesis ranked above a real one.
			name:          "empty first, real second",
			resps:         []*speechpb.StreamingRecognizeResponse{finalResult(alt("", 0), alt("turn on the light", 0.8))},
			wantFinals:    1,
			wantDelivered: []string{"turn on the light"},
			wantReason:    "success",
		},
		{
			name:          "normal final",
			resps:         []*speechpb.StreamingRecognizeResponse{finalResult(alt("hello world", 0.9))},
			wantFinals:    1,
			wantDelivered: []string{"hello world"},
			wantReason:    "success",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := &fakeTarget{}
			s := &googleCloudSTT{
				logger:           logging.NewTestLogger(t),
				cfg:              &Config{},
				transcriptTarget: target,
			}
			sess := &sessionState{}
			done := make(chan struct{})
			s.receiveFromGoogle(context.Background(), &fakeStream{resps: tc.resps}, sess, done)
			<-done

			if sess.finalCount != tc.wantFinals {
				t.Errorf("finalCount = %d, want %d", sess.finalCount, tc.wantFinals)
			}
			if len(target.delivered) != len(tc.wantDelivered) {
				t.Fatalf("delivered = %v, want %v", target.delivered, tc.wantDelivered)
			}
			for i := range tc.wantDelivered {
				if target.delivered[i] != tc.wantDelivered[i] {
					t.Errorf("delivered[%d] = %q, want %q", i, target.delivered[i], tc.wantDelivered[i])
				}
			}
			if got := normalizeCloseReason("segment-end sentinel", sess); got != tc.wantReason {
				t.Errorf("close reason = %q, want %q", got, tc.wantReason)
			}
		})
	}
}

func TestNormalizeCloseReason(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reason string
		sess   *sessionState
		want   string
	}{
		// A session whose only finals were empty leaves finalCount at 0 and
		// must close as no_result, not success.
		{"segment end without usable finals", "segment-end sentinel", &sessionState{}, "no_result"},
		{"segment end with finals", "segment-end sentinel", &sessionState{finalCount: 1}, "success"},
		{"recv error trumps finals", "segment-end sentinel", &sessionState{finalCount: 1, recvErr: errors.New("boom")}, "recv_error"},
		{"audio_in closed", "audio_in channel closed", &sessionState{}, "context_cancelled"},
		{"send error", "send error", &sessionState{}, "send_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeCloseReason(tc.reason, tc.sess); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}
