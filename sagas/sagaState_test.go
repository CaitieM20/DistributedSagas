package sagas

import (
	"bytes"
	"fmt"
	"testing"
)

func TestSagaStateFactory(t *testing.T) {

	sagaId := "testSaga"
	job := []byte{0, 1, 2, 3, 4, 5}

	state, _ := makeSagaState("testSaga", job)
	if state.sagaId != sagaId {
		t.Error(fmt.Sprintf("SagaState SagaId should be the same as the SagaId passed to Factory Method"))
	}

	if !bytes.Equal(state.job, job) {
		t.Error(fmt.Sprintf("SagaState Job should be the same as the supplied Job passed to Factory Method"))
	}
}


