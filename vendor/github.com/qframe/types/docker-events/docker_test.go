package qtypes_docker_events

import (
	"testing"
	"time"
	"github.com/qframe/types/messages"

	"github.com/docker/docker/api/types/events"

	"github.com/stretchr/testify/assert"
	"github.com/qframe/types/helper"
)

func TestNewDockerEvent(t *testing.T) {
	ts := time.Unix(1499156134, 123124)
	b := qtypes_messages.NewTimedBase("src1", ts)
	event := events.Message{
		Actor: events.Actor{ID: "123"},
		Action: "start",
		Type: "container",
	}
	exp := DockerEvent{
		Base: b,
		Message: "container.start",
		Event: event,
	}
	got := NewDockerEvent(b, event)
	assert.Equal(t, exp, got)
}


func TestDockerEvent_ToJSON(t *testing.T) {
	ts := time.Unix(1499156134, 123124)
	b := qtypes_messages.NewTimedBase("src1", ts)
	event := events.Message{
		Actor: events.Actor{ID: "123"},
		Action: "start",
		Type: "container",
	}
	de := NewDockerEvent(b, event)
	exp := map[string]interface{}{
		"base_version": b.BaseVersion,
		"id": "",
		"time": ts.String(),
		"time_unix_nano": ts.UnixNano(),
		"source_id": 0,
		"source_path": []string{"src1"},
		"source_success": true,
		"tags": map[string]string{},
		"message": "container.start",
		"event": event,
	}
	got := de.EventToJSON()
	assert.Equal(t, exp["time"], got["time"])
	res := qtypes_helper.CompareMap(exp, got)
	assert.True(t, res, "Not deeply equal")
}