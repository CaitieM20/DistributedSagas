package sagas

import (
	"fmt"
)

type flag byte

const (
	TaskStarted flag = 1 << iota
	TaskCompleted
	CompTaskStarted
	CompTaskCompleted
)

//
// Optional additional data about tasks to be committed to the log
// This is Opaque to SagaState, but useful data to persist for
// results or debugging
//
type taskData struct {
	taskStart     []byte
	taskEnd       []byte
	compTaskStart []byte
	compTaskEnd   []byte
}

//
// Data Structure representation of the current state of the Saga.
//
type SagaState struct {
	// unique identifier of the saga
	sagaId string

	// description of saga job
	job    []byte

	// map of taskId to task data supplied when committing
	// startTask, endTask, startCompTask, endCompTask messages
	taskData map[string]*taskData

	// map of taskId to Flag specifying task progress
	taskState map[string]flag

	//bool if AbortSaga message logged
	sagaAborted bool

	//bool if EndSaga message logged
	sagaCompleted bool
}

//
// Initialize a Default Empty Saga
//
func initializeSagaState() *SagaState {
	return &SagaState{
		sagaId:        "",
		job:           nil,
		taskState:     make(map[string]flag),
		taskData:      make(map[string]*taskData),
		sagaAborted:   false,
		sagaCompleted: false,
	}
}

//
// Returns a lists of task ids associated with this Saga
//
func (state *SagaState) getTaskIds() []string {
	taskIds := make([]string, 0, len(state.taskState))

	for id, _ := range state.taskState {
		taskIds = append(taskIds, id)
	}

	return taskIds
}

//
// Returns true if the specified Task has been started,
// fasle otherwise
//
func (state *SagaState) isTaskStarted(taskId string) bool {
	flags, _ := state.taskState[taskId]
	return flags&TaskStarted != 0
}

//
// Get Data Associated with Start Task, supplied as
// Part of the StartTask Message
//
func (state *SagaState) getStartTaskData(taskId string) []byte {
	data, ok := state.taskData[taskId]
	if ok {
		return data.taskStart
	} else {
		return nil
	}
}

//
// Returns true if the specified Task has been completed,
// false otherwise
//
func (state *SagaState) isTaskCompleted(taskId string) bool {
	flags, _ := state.taskState[taskId]
	return flags&TaskCompleted != 0
}

//
// Get Data Associated with End Task, supplied as
// Part of the EndTask Message
///
func (state *SagaState) getEndTaskData(taskId string) []byte {
	data, ok := state.taskData[taskId]
	if ok {
		return data.taskEnd
	} else {
		return nil
	}
}

//
// Returns true if the specified Compensating Task has been started,
// fasle otherwise
//
func (state *SagaState) isCompTaskStarted(taskId string) bool {
	flags, _ := state.taskState[taskId]
	return flags&CompTaskStarted != 0
}

//
// Get Data Associated with Starting Comp Task, supplied as
// Part of the StartCompTask Message
//
func (state *SagaState) getStartCompTaskData(taskId string) []byte {
	data, ok := state.taskData[taskId]
	if ok {
		return data.compTaskStart
	} else {
		return nil
	}
}

//
// Returns true if the specified Compensating Task has been completed,
// fasle otherwise
//
func (state *SagaState) isCompTaskCompleted(taskId string) bool {
	flags, _ := state.taskState[taskId]
	return flags&CompTaskCompleted != 0
}

//
// Get Data Associated with End Comp Task, supplied as
// Part of the EndCompTask Message
//
func (state *SagaState) getEndCompTaskData(taskId string) []byte {
	data, ok := state.taskData[taskId]
	if ok {
		return data.compTaskEnd
	} else {
		return nil
	}
}

//
// Returns true if this Saga has been Aborted, false otherwise
//
func (state *SagaState) isSagaAborted() bool {
	return state.sagaAborted
}

//
// Returns true if this Saga has been Completed, false otherwise
//
func (state *SagaState) isSagaCompleted() bool {
	return state.sagaCompleted
}

//
// Add the data for the specified message type to the task metadata fields.
// This Data is stored in the SagaState and persisted to durable saga log, so it
// can be recovered.  It is opaque to sagas but useful to persist for applications.
//
func (state *SagaState) addTaskData(taskId string, msgType SagaMessageType, data []byte) {

	tData, ok := state.taskData[taskId]
	if !ok {
		tData = &taskData{}
		state.taskData[taskId] = tData
	}

	switch msgType {
	case StartTask:
		state.taskData[taskId].taskStart = data

	case EndTask:
		state.taskData[taskId].taskEnd = data

	case StartCompTask:
		state.taskData[taskId].compTaskStart = data

	case EndCompTask:
		state.taskData[taskId].compTaskEnd = data
	}
}


//
// Initialize a SagaState for the specified saga, and default data.
//
func makeSagaState(sagaId string, job []byte) (*SagaState, error) {

	state := initializeSagaState()

	err := validateSagaId(sagaId)
	if err != nil {
		return nil, err
	}

	state.sagaId = sagaId
	state.job = job

	return state, nil
}

//
// Custom ToString function for SagaState
//
func (state *SagaState) String() string {

	fmtString := "{ SagaId: %v, " +
		"SagaAborted: %v, " +
		"SagaCompleted: %v, " +
		"Tasks: [ "

	for _, id := range state.getTaskIds() {

		taskState := ""

		if state.isTaskStarted(id) {
			taskState += "Started|"
		}
		if state.isTaskCompleted(id) {
			taskState += "Completed|"
		}
		if state.isCompTaskStarted(id) {
			taskState += "CompTaskStarted|"
		}
		if state.isCompTaskCompleted(id) {
			taskState += "CompTaskCompleted|"
		}

		// remove trailing slash
		if len(taskState) >= 1 {
			taskState = taskState[0 : len(taskState)-1]
		}

		fmtString += fmt.Sprintf("%v: %s, ", id, taskState)
	}

	fmtString += "]"

	return fmt.Sprintf(
		fmtString,
		state.sagaId,
		state.isSagaAborted(),
		state.isSagaCompleted(),
	)
} 
