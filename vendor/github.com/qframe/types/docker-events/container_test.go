package qtypes_docker_events

import (
	"testing"
	"time"
	"github.com/stretchr/testify/assert"

	"github.com/docker/docker/api/types/events"

	"github.com/docker/docker/api/types"
	"github.com/qframe/types/messages"
	"github.com/qframe/types/helper"
)

func TestContainerEvent_ToJSON(t *testing.T) {
	ts := time.Unix(1499156134, 123124)
	b := qtypes_messages.NewTimedBase("src1", ts)
	cbase := types.ContainerJSONBase{
		ID: "abcde",
	}
	cnt := types.ContainerJSON{
		ContainerJSONBase: &cbase,
	}
	event := events.Message{
		Actor: events.Actor{ID: "123"},
		Action: "start",
		Type: "container",
	}
	de := NewDockerEvent(b, event)
	ce := NewContainerEvent(de, cnt)
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
		"container": cnt,
	}
	got := ce.ContainerToJSON()
	assert.Equal(t, exp["time"], got["time"])
	res := qtypes_helper.CompareMap(exp, got)
	assert.True(t, res, "Not deeply equal")
}


