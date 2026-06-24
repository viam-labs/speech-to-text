package utils

import (
	"context"
	"encoding/base64"
	"errors"
	"sync"
	"testing"
	"time"

	"go.viam.com/rdk/app"
	"go.viam.com/rdk/data"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
	"go.viam.com/test"
)

// fakeUploader is a hand-rolled stub for binaryUploader. It records the most
// recent call args and returns the configured ID + error.
type fakeUploader struct {
	mu          sync.Mutex
	returnID    string
	returnErr   error
	calls       int
	lastWAV     []byte
	lastTags    []string
	lastPartID  string
	lastReqTime *[2]time.Time
}

func (f *fakeUploader) BinaryDataCaptureUpload(
	_ context.Context,
	binaryData []byte,
	partID string,
	_ string, // componentType
	_ string, // componentName
	_ string, // methodName
	_ string, // fileExtension
	options *app.BinaryDataCaptureUploadOptions,
) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastWAV = binaryData
	f.lastPartID = partID
	if options != nil {
		f.lastTags = options.Tags
		f.lastReqTime = options.DataRequestTimes
	}
	return f.returnID, f.returnErr
}

func (f *fakeUploader) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// newTestSensor constructs a sessionSensor wired with the given uploader,
// bypassing newSessionSensor (which reads env vars and contacts Viam app).
func newTestSensor(uploader binaryUploader, maxQueueSize int) *sessionSensor {
	return &sessionSensor{
		name:     resource.Name{Name: "test-sensor"},
		logger:   logging.NewTestLogger(&testing.T{}),
		cfg:      &SessionSensorConfig{MaxQueueSize: maxQueueSize},
		uploader: uploader,
		partID:   "test-part",
	}
}

func sampleReading(captureID string) SessionReading {
	now := time.Now().UTC()
	return SessionReading{
		CaptureID:      captureID,
		Transcript:     "hello",
		Confidence:     0.9,
		CloseReason:    "success",
		AudioSentBytes: 1024,
		WAV:            []byte{0x52, 0x49, 0x46, 0x46}, // fake WAV header bytes
		StartTime:      now.Add(-time.Second),
		EndTimeUs:      now.UnixMicro(),
		LanguageCode:   "en-US",
		Model:          "latest_short",
		ResponseCount:  3,
		FinalCount:     1,
		InterimCount:   2,
	}
}

func TestReadingsEmptyQueueReturnsErrNoCaptureToStore(t *testing.T) {
	s := newTestSensor(&fakeUploader{returnID: "ignored"}, 100)
	_, err := s.Readings(context.Background(), data.FromDMExtraMap)
	test.That(t, err, test.ShouldEqual, data.ErrNoCaptureToStore)
}

func TestPushSessionQueuesAndReadingsReturnsIt(t *testing.T) {
	up := &fakeUploader{returnID: "binid-1"}
	s := newTestSensor(up, 100)

	binID, err := s.PushSession(context.Background(), sampleReading("cap-1"))
	test.That(t, err, test.ShouldBeNil)
	test.That(t, binID, test.ShouldEqual, "binid-1")
	test.That(t, up.callCount(), test.ShouldEqual, 1)

	got, err := s.Readings(context.Background(), data.FromDMExtraMap)
	test.That(t, err, test.ShouldBeNil)
	test.That(t, got["capture_id"], test.ShouldEqual, "cap-1")
	test.That(t, got["binary_data_id"], test.ShouldEqual, "binid-1")
	test.That(t, got["transcript"], test.ShouldEqual, "hello")
	test.That(t, got["close_reason"], test.ShouldEqual, "success")

	// Queue should be empty again.
	_, err = s.Readings(context.Background(), data.FromDMExtraMap)
	test.That(t, err, test.ShouldEqual, data.ErrNoCaptureToStore)
}

func TestPushSessionFIFOAcrossMultiplePushes(t *testing.T) {
	s := newTestSensor(&fakeUploader{returnID: "binid"}, 100)
	for _, id := range []string{"a", "b", "c"} {
		_, err := s.PushSession(context.Background(), sampleReading(id))
		test.That(t, err, test.ShouldBeNil)
	}
	for _, want := range []string{"a", "b", "c"} {
		got, err := s.Readings(context.Background(), data.FromDMExtraMap)
		test.That(t, err, test.ShouldBeNil)
		test.That(t, got["capture_id"], test.ShouldEqual, want)
	}
	_, err := s.Readings(context.Background(), data.FromDMExtraMap)
	test.That(t, err, test.ShouldEqual, data.ErrNoCaptureToStore)
}

func TestPushSessionUploadErrorQueuesRowWithEmptyBinaryID(t *testing.T) {
	up := &fakeUploader{returnID: "ignored", returnErr: errors.New("network down")}
	s := newTestSensor(up, 100)

	binID, err := s.PushSession(context.Background(), sampleReading("cap-err"))
	test.That(t, err, test.ShouldNotBeNil)
	test.That(t, binID, test.ShouldEqual, "")

	got, err := s.Readings(context.Background(), data.FromDMExtraMap)
	test.That(t, err, test.ShouldBeNil)
	test.That(t, got["capture_id"], test.ShouldEqual, "cap-err")
	test.That(t, got["binary_data_id"], test.ShouldEqual, "")
}

func TestPushSessionMaxQueueSizeDropsOldest(t *testing.T) {
	s := newTestSensor(&fakeUploader{returnID: "binid"}, 2)

	for _, id := range []string{"a", "b", "c", "d"} {
		_, err := s.PushSession(context.Background(), sampleReading(id))
		test.That(t, err, test.ShouldBeNil)
	}

	// Queue capacity 2 — only the two newest survive.
	got1, _ := s.Readings(context.Background(), data.FromDMExtraMap)
	got2, _ := s.Readings(context.Background(), data.FromDMExtraMap)
	_, err := s.Readings(context.Background(), data.FromDMExtraMap)

	test.That(t, got1["capture_id"], test.ShouldEqual, "c")
	test.That(t, got2["capture_id"], test.ShouldEqual, "d")
	test.That(t, err, test.ShouldEqual, data.ErrNoCaptureToStore)
}

func TestPushSessionTagsAndPartIDForwardedToUploader(t *testing.T) {
	up := &fakeUploader{returnID: "binid"}
	s := newTestSensor(up, 100)

	_, err := s.PushSession(context.Background(), sampleReading("cap-tags"))
	test.That(t, err, test.ShouldBeNil)

	test.That(t, up.lastPartID, test.ShouldEqual, "test-part")
	test.That(t, len(up.lastTags), test.ShouldEqual, 3)
	test.That(t, up.lastTags[0], test.ShouldEqual, "voice_session")
	test.That(t, up.lastTags[1], test.ShouldEqual, "capture_cap-tags")
	test.That(t, up.lastTags[2], test.ShouldEqual, "success") // closeReasonTag of "success"
}

func TestDoCommandPushSessionRoundTrip(t *testing.T) {
	up := &fakeUploader{returnID: "binid-doc"}
	s := newTestSensor(up, 100)

	start := time.Now().UTC().Add(-time.Second)
	end := time.Now().UTC()
	wavBytes := []byte("RIFFfake")
	cmd := map[string]interface{}{
		"command":          "push_session",
		"capture_id":       "cap-doc",
		"transcript":       "doc transcript",
		"confidence":       0.75,
		"close_reason":     "success",
		"audio_sent_bytes": float64(2048),
		"audio_wav_b64":    base64.StdEncoding.EncodeToString(wavBytes),
		"start_time":       start.Format(time.RFC3339Nano),
		"end_time_us":      float64(end.UnixMicro()),
		"language_code":    "en-US",
	}

	resp, err := s.DoCommand(context.Background(), cmd)
	test.That(t, err, test.ShouldBeNil)
	test.That(t, resp["capture_id"], test.ShouldEqual, "cap-doc")
	test.That(t, resp["binary_data_id"], test.ShouldEqual, "binid-doc")

	test.That(t, up.callCount(), test.ShouldEqual, 1)
	test.That(t, string(up.lastWAV), test.ShouldEqual, "RIFFfake")

	got, err := s.Readings(context.Background(), data.FromDMExtraMap)
	test.That(t, err, test.ShouldBeNil)
	test.That(t, got["transcript"], test.ShouldEqual, "doc transcript")
	test.That(t, got["binary_data_id"], test.ShouldEqual, "binid-doc")
}

func TestDoCommandUnknownCommandReturnsError(t *testing.T) {
	s := newTestSensor(&fakeUploader{returnID: "binid"}, 100)
	_, err := s.DoCommand(context.Background(), map[string]interface{}{"command": "wat"})
	test.That(t, err, test.ShouldNotBeNil)
}

func TestReadingsNonDMCallReturnsLastReadingWithoutDraining(t *testing.T) {
	// Non-DM callers (UI live preview, Test panel) should see the most recent
	// reading on every poll without consuming the queue. DM-flagged polls
	// still get strict queue-pop semantics.
	s := newTestSensor(&fakeUploader{returnID: "binid"}, 100)

	// Empty: should still raise ErrNoCaptureToStore for non-DM callers too.
	_, err := s.Readings(context.Background(), nil)
	test.That(t, err, test.ShouldEqual, data.ErrNoCaptureToStore)

	_, err = s.PushSession(context.Background(), sampleReading("ui-1"))
	test.That(t, err, test.ShouldBeNil)

	// Multiple non-DM polls all return the same row, never empty.
	for i := 0; i < 3; i++ {
		got, err := s.Readings(context.Background(), nil)
		test.That(t, err, test.ShouldBeNil)
		test.That(t, got["capture_id"], test.ShouldEqual, "ui-1")
	}

	// DM-flagged poll pops the row from the queue.
	got, err := s.Readings(context.Background(), data.FromDMExtraMap)
	test.That(t, err, test.ShouldBeNil)
	test.That(t, got["capture_id"], test.ShouldEqual, "ui-1")

	// Queue now empty, but non-DM polls still see the sticky last reading.
	got, err = s.Readings(context.Background(), nil)
	test.That(t, err, test.ShouldBeNil)
	test.That(t, got["capture_id"], test.ShouldEqual, "ui-1")
}

func TestPushSessionWithoutUploaderStillQueuesRow(t *testing.T) {
	// Simulate the "no Viam app creds" path: uploader is nil. PushSession
	// should still queue the row with an empty binary_data_id.
	s := newTestSensor(nil, 100)

	binID, err := s.PushSession(context.Background(), sampleReading("cap-no-upload"))
	test.That(t, err, test.ShouldBeNil)
	test.That(t, binID, test.ShouldEqual, "")

	got, err := s.Readings(context.Background(), data.FromDMExtraMap)
	test.That(t, err, test.ShouldBeNil)
	test.That(t, got["capture_id"], test.ShouldEqual, "cap-no-upload")
	test.That(t, got["binary_data_id"], test.ShouldEqual, "")
}

func TestValidateAcceptsZeroAndPositiveMaxQueueSize(t *testing.T) {
	for _, v := range []int{0, 1, 1000} {
		cfg := &SessionSensorConfig{MaxQueueSize: v}
		_, _, err := cfg.Validate("test")
		test.That(t, err, test.ShouldBeNil)
	}
}

func TestValidateRejectsNegativeMaxQueueSize(t *testing.T) {
	cfg := &SessionSensorConfig{MaxQueueSize: -1}
	_, _, err := cfg.Validate("test")
	test.That(t, err, test.ShouldNotBeNil)
	test.That(t, err.Error(), test.ShouldContainSubstring, "max_queue_size")
}

func TestValidateRejectsEmptyDatasetID(t *testing.T) {
	cfg := &SessionSensorConfig{DatasetIDs: []string{"good", "", "alsogood"}}
	_, _, err := cfg.Validate("test")
	test.That(t, err, test.ShouldNotBeNil)
	test.That(t, err.Error(), test.ShouldContainSubstring, "dataset_ids[1]")
}

func TestValidateAcceptsAllNonEmptyDatasetIDs(t *testing.T) {
	cfg := &SessionSensorConfig{DatasetIDs: []string{"a", "b"}}
	_, _, err := cfg.Validate("test")
	test.That(t, err, test.ShouldBeNil)
}
