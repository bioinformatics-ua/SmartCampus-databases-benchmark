package main

import (
	"bytes"
	"io"
	"testing"
)

func TestBinaryRoundTrip(t *testing.T) {
	var b bytes.Buffer
	want := Event{"user-a", "ap, =b", 1735689600, -61.25}
	if e := writeEvent(&b, want); e != nil {
		t.Fatal(e)
	}
	got, e := readEvent(&b)
	if e != nil || got != want {
		t.Fatalf("%+v %v", got, e)
	}
	if _, e = readEvent(&b); e != io.EOF {
		t.Fatal(e)
	}
}
func TestTruncation(t *testing.T) {
	var b bytes.Buffer
	writeEvent(&b, Event{"u", "s", 1735689600, -50})
	data := b.Bytes()
	if _, e := readEvent(bytes.NewReader(data[:len(data)-1])); e == nil {
		t.Fatal("truncated record accepted")
	}
}
func TestTransforms(t *testing.T) {
	e := Event{"user", "ssid", 1735689600, -70}
	a := transform(e, "fragmented")
	if a.TS != e.TS || a.SSID != e.SSID || a.User == e.User {
		t.Fatal(a)
	}
	if transform(e, "base") != e {
		t.Fatal("base changed")
	}
}
func TestLineProtocolEscapes(t *testing.T) {
	s := ilp([]Event{{"user a", "ap,1", 1735689600, -60}})
	if !bytes.Contains([]byte(s), []byte("user_id=user\\ a,ssid=ap\\,1")) {
		t.Fatal(s)
	}
}
