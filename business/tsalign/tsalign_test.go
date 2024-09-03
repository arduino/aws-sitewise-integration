package tsalign

import (
	"testing"
	"time"

	iotclient "github.com/arduino/iot-client-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestPartitionData(t *testing.T) {

	response := generateSamples(10)
	partitions := partitionResults(response)
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, 10, len(partitions[0].ts))
	assert.Equal(t, 10, len(partitions[0].values))

	response = generateSamples(35)
	partitions = partitionResults(response)
	assert.Equal(t, 4, len(partitions))
	assert.Equal(t, 10, len(partitions[0].ts))
	assert.Equal(t, 10, len(partitions[0].values))
	assert.Equal(t, 5, len(partitions[3].ts))
	assert.Equal(t, 5, len(partitions[3].values))

	response = generateSamples(60)
	partitions = partitionResults(response)
	assert.Equal(t, 6, len(partitions))
	for _, p := range partitions {
		assert.Equal(t, 10, len(p.ts))
		assert.Equal(t, 10, len(p.values))
	}
}

func generateSamples(howMany int) iotclient.ArduinoSeriesResponse {
	values := []float64{}
	ts := []time.Time{}
	now := time.Now()
	for i := 0; i < howMany; i++ {
		values = append(values, float64(i))
		ts = append(ts, now.Add(time.Duration(-i)*time.Second))
	}
	return iotclient.ArduinoSeriesResponse{
		Values: values,
		Times:  ts,
	}
}
