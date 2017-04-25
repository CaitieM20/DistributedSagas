package inMemLog

import (
	"errors"
	"fmt"
	"github.com/caitiem20/DistributedSagas/sagas"
	"sync"
)

/*
 * In Memory Implementation of a Saga Log, DOES NOT durably persist messages.
 * This is for local testing purposes.
 */
type inMemorySagaLog struct {
	sagas map[string][]sagas.SagaMessage
	mutex sync.RWMutex
}

/*
 * Returns an Instance of a Saga based on an InMemorySagaLog
 */
func MakeInMemorySagaCoordinator() sagas.SagaCoordinator {
	inMemLog := &inMemorySagaLog{
		sagas: make(map[string][]sagas.SagaMessage),
		mutex: sync.RWMutex{},
	}
	return sagas.MakeSagaCoordinator(inMemLog)
}

func (log *inMemorySagaLog) LogMessage(msg sagas.SagaMessage) error {

	log.mutex.Lock()
	defer log.mutex.Unlock()

	fmt.Println(fmt.Sprintf("Saga %s: %s %s", msg.GetSagaId(), msg.GetMessageType().String(), msg.GetTaskId()))
	sagaId := msg.GetSagaId()

	msgs, ok := log.sagas[sagaId]
	if !ok {
		return errors.New(fmt.Sprintf("Saga: %s is not Started yet.", msg.GetSagaId()))
	}

	log.sagas[sagaId] = append(msgs, msg)
	return nil
}

func (log *inMemorySagaLog) StartSaga(sagaId string, job []byte) error {

	log.mutex.Lock()
	defer log.mutex.Unlock()

	fmt.Println(fmt.Sprintf("Start Saga %s", sagaId))

	startMsg := sagas.MakeStartSagaMessage(sagaId, job)
	log.sagas[sagaId] = []sagas.SagaMessage{startMsg}

	return nil
}

func (log *inMemorySagaLog) GetMessages(sagaId string) ([]sagas.SagaMessage, error) {

	log.mutex.RLock()
	defer log.mutex.RUnlock()

	msgs, ok := log.sagas[sagaId]

	if ok {
		return msgs, nil
	} else {
		return nil, nil
	}
}

/*
 * Returns all Sagas Started since this InMemory Saga was created
 */
func (log *inMemorySagaLog) GetActiveSagas() ([]string, error) {
	log.mutex.RLock()
	defer log.mutex.RUnlock()

	keys := make([]string, 0, len(log.sagas))

	for key, _ := range log.sagas {
		keys = append(keys, key)
	}

	return keys, nil
}
