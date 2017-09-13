package qtypes_helper

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestCompareMap(t *testing.T) {
	m := map[string]interface{}{
		"name": "name",
		"list": []string{"1","2"},
		"map": map[string]string{"key": "val"},
		"default": []float64{1.2,1.3},
	}
	assert.True(t, CompareMap(m,m))
	fail1 := map[string]interface{}{
		"list": []string{"1","2"},
		"map": map[string]string{"key": "val"},
	}
	assert.False(t, CompareMap(m,fail1))
	fail2 := map[string]interface{}{
		"name": "name1",
		"list": []string{"1","2"},
		"map": map[string]string{"key": "val"},
		"default": []float64{1.2,1.3},
	}
	assert.False(t, CompareMap(m,fail2))
	fail3 := map[string]interface{}{
		"name": "name",
		"list": []string{"1","2","3"},
		"map": map[string]string{"key": "val"},
		"default": []float64{1.2,1.3},
	}
	assert.False(t, CompareMap(m,fail3))
	fail4 := map[string]interface{}{
		"name": "name",
		"list": []string{"1","2"},
		"map": map[string]string{"key": "val1"},
		"default": []float64{1.2,1.3},
	}
	assert.False(t, CompareMap(m,fail4))
	fail5 := map[string]interface{}{
		"name": "name",
		"list": []string{"1","2"},
		"map": map[string]string{"key": "val"},
		"default": []float64{1.2,2.4},
	}
	assert.False(t, CompareMap(m,fail5))
}