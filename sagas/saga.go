package sagas

import (
	"sync"
)

// Concurrent Object Representing a Saga, to allow for the concurrent execution of tasks
// Methods update the state of the saga.  
type Saga struct {
	id       string
	log      SagaLog
	state    *SagaState
	updateCh chan sagaUpdate
	mutex    sync.RWMutex
}

// Start a New Saga.  Logs a Start Saga Message to the SagaLog
// returns a Saga, or an error if one occurs
func newSaga(sagaId string, job []byte, log SagaLog) (*Saga, error) {

	state, err := makeSagaState(sagaId, job)
	if err != nil {
		return nil, err
	}

	err = log.StartSaga(sagaId, job)
	if err != nil {
		return nil, err
	}

	updateCh := make(chan sagaUpdate, 0)

	s := &Saga{
		id:       sagaId,
		log:      log,
		state:    state,
		updateCh: updateCh,
		mutex:    sync.RWMutex{},
	}

	go s.updateSagaStateLoop()

	return s, nil
}

// Rehydrate a saga from a specified SagaState, does not write
// to SagaLog assumes that this is a recovered saga.
func rehydrateSaga(sagaId string, state *SagaState, log SagaLog) *Saga {
	updateCh := make(chan sagaUpdate, 0)
	s := &Saga{
		id:       sagaId,
		log:      log,
		state:    state,
		updateCh: updateCh,
		mutex:    sync.RWMutex{},
	}

	if !state.isSagaCompleted() {
		go s.updateSagaStateLoop()
	}

	return s
}

// Getter Methods for Saga State to ensure
// concurrent access is safe.  
func (s *Saga) SagaId() string {
	return s.id
}

func (s *Saga) Job() []byte {
	s.mutex.RLock()
	r := s.state.job
	s.mutex.RUnlock()
	return r
}

func (s *Saga) GetTaskIds() []string {
	s.mutex.RLock()
	r := s.state.getTaskIds()
	s.mutex.RUnlock()
	return r
}

func (s *Saga) IsTaskStarted(taskId string) bool {
	s.mutex.RLock()
	r := s.state.isTaskStarted(taskId)
	s.mutex.RUnlock()
	return r
}

func (s *Saga) GetStartTaskData(taskId string) []byte {
	s.mutex.RLock()
	r := s.state.getStartTaskData(taskId)
	s.mutex.RUnlock()
	return r
}

func (s *Saga) IsTaskCompleted(taskId string) bool {
	s.mutex.RLock()
	r := s.state.isTaskCompleted(taskId)
	s.mutex.RUnlock()
	return r
}

func (s *Saga) GetEndTaskData(taskId string) []byte {
	s.mutex.RLock()
	r := s.state.getEndTaskData(taskId)
	s.mutex.RUnlock()
	return r
}

func (s *Saga)  IsCompTaskStarted(taskId string) bool {
	s.mutex.RLock()
	r := s.state.isCompTaskStarted(taskId)
	s.mutex.RUnlock()
	return r
}

func (s *Saga) GetStartCompTaskData(taskId string) []byte {
	s.mutex.RLock()
	r := s.state.getStartCompTaskData(taskId)
	s.mutex.RUnlock()
	return r
}

func (s *Saga) IsCompTaskCompleted(taskId string) bool {
	s.mutex.RLock()
	r := s.state.isCompTaskCompleted(taskId)
	s.mutex.RUnlock()
	return r
}

func (s *Saga) GetEndCompTaskData(taskId string) []byte {
	s.mutex.RLock()
	r := s.state.getEndCompTaskData(taskId)
	s.mutex.RUnlock()
	return r
}

func (s *Saga) IsSagaAborted() bool {
	s.mutex.RLock()
	r := s.state.isSagaAborted()
	s.mutex.RUnlock()
	return r
}

func (s *Saga) IsSagaCompleted() bool {
	s.mutex.RLock()
	r := s.state.isSagaCompleted()
	s.mutex.RUnlock()
	return r
}

//
// Log an End Saga Message to the log, returns updated SagaState
// Returns the resulting SagaState or an error if it fails
//
// Once EndSaga is succesfully called, trying to log additional
// messages will result in a panic.
func (s *Saga) EndSaga() error {
	return s.updateSagaState(MakeEndSagaMessage(s.id))
}

//
// Log an AbortSaga message.  This indicates that the
// Saga has failed and all execution should be stopped
// and compensating transactions should be applied.
//
// Returns an error if it fails
//
func (s *Saga) AbortSaga() error {
	return s.updateSagaState(MakeAbortSagaMessage(s.id))
}

//
// Log a StartTask Message to the log.  Returns
// an error if it fails.
//
// StartTask is idempotent with respect to sagaId & taskId.  If
// the data passed changes the last written StartTask message will win
//
// Returns an error if it fails
//
func (s *Saga) StartTask(taskId string, data []byte) error {
	return s.updateSagaState(MakeStartTaskMessage(s.id, taskId, data))
}

//
// Log an EndTask Message to the log.  Indicates that this task
// has been successfully completed. Returns an error if it fails.
//
// EndTask is idempotent with respect to sagaId & taskId.  If
// the data passed changes the last written EndTask message will win
//
// Returns an error if it fails
//
func (s *Saga) EndTask(taskId string, results []byte) error {
	return s.updateSagaState(MakeEndTaskMessage(s.id, taskId, results))
}

//
// Log a Start Compensating Task Message to the log. Should only be logged after a Saga
// has been avoided and in Rollback Recovery Mode. Should not be used in ForwardRecovery Mode
// returns an error if it fails
//
// StartCompTask is idempotent with respect to sagaId & taskId.  If
// the data passed changes the last written StartCompTask message will win
//
// Returns an error if it fails
//
func (s *Saga) StartCompensatingTask(taskId string, data []byte) error {
	return s.updateSagaState(MakeStartCompTaskMessage(s.id, taskId, data))
}

//
// Log an End Compensating Task Message to the log when a Compensating Task
// has been successfully completed. Returns an error if it fails.
//
// EndCompTask is idempotent with respect to sagaId & taskId.  If
// the data passed changes the last written EndCompTask message will win
//
// Returns an error if it fails
//
func (s *Saga) EndCompensatingTask(taskId string, results []byte) error {
	return s.updateSagaState(MakeEndCompTaskMessage(s.id, taskId, results))
}

// adds a message for updateSagaStateLoop to execute to the channel for the
// specified saga.  Returns after the message has been applied
func (s *Saga) updateSagaState(msg SagaMessage) error {
	resultCh := make(chan error, 0)
	s.updateCh <- sagaUpdate{
		msg:      msg,
		resultCh: resultCh,
	}

	result := <-resultCh

	// after we successfully log an EndSaga message close the channel
	// no more messages should be logged
	if msg.msgType == EndSaga {
		close(s.updateCh)
	}

	return result
}

// updateSagaStateLoop that is executed inside of a single go routine.  There
// is one per saga currently executing.  This ensures all updates are applied
// in order to a saga.  Also controls access to the SagaState so its is
// updated in a thread safe manner
func (s *Saga) updateSagaStateLoop() {
	for update := range s.updateCh {
		s.itr(update)
	}
}

func (s *Saga) itr(update sagaUpdate) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	err := logMessage(s.state, update.msg, s.log)

	if err == nil {
		update.resultCh <- nil
	} else {
		update.resultCh <- err
	}
}

type sagaUpdate struct {
	msg      SagaMessage
	resultCh chan error
}

//
// logs the specified message durably to the SagaLog & updates internal state if its a valid state transition
// returns an error if the update was invalid or there was an error writing to the durable sagaLog
//
func logMessage(state *SagaState, msg SagaMessage, log SagaLog) (error) {

	//verify that the applied message results in a valid state
	err := validateSagaUpdate(state, msg)
	if err != nil {
		return err
	}

	//try durably storing the message
	err = log.LogMessage(msg)
	if err != nil {
		return err
	}

	// actually update the in-memory state
	updateSagaState(state, msg)
	return nil
}


