package internal

import (
	"testing"
	"time"
)

func TestCanonicalJSONNormalizesWhitespaceAndObjectKeys(t *testing.T) {
	a, err := canonicalJSON([]byte(`{"z":2,"a":{"y":1,"x":0}}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := canonicalJSON([]byte("{ \"a\": {\"x\":0,\"y\":1}, \"z\":2 }"))
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) {
		t.Fatalf("canonical encodings differ: %s != %s", a, b)
	}
}
func TestCanonicalJSONRequiresObject(t *testing.T) {
	for _, raw := range []string{`null`, `[]`, `true`, `1`, `"text"`, `{`} {
		if _, err := canonicalJSON([]byte(raw)); err == nil {
			t.Fatalf("expected %q to fail", raw)
		}
	}
}
func TestRetryDelayIsExponentialAndBounded(t *testing.T) {
	cases := []struct {
		attempt int
		want    time.Duration
	}{{0, time.Second}, {1, time.Second}, {2, 2 * time.Second}, {3, 4 * time.Second}, {7, 60 * time.Second}, {100, 60 * time.Second}}
	for _, tc := range cases {
		if got := retryDelay(tc.attempt); got != tc.want {
			t.Errorf("attempt %d: got %s want %s", tc.attempt, got, tc.want)
		}
	}
}
func TestJSONEqual(t *testing.T) {
	if !jsonEqual([]byte(`{"a":1,"b":2}`), []byte(`{ "b":2,"a":1 }`)) {
		t.Fatal("equivalent JSON should compare equal")
	}
	if jsonEqual([]byte(`{"a":1}`), []byte(`{"a":2}`)) {
		t.Fatal("different JSON must not compare equal")
	}
}
