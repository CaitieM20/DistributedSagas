package sagas

type SagaMessageType int

const (
	StartSaga SagaMessageType = iota
	EndSaga
	AbortSaga
	StartTask
	EndTask
	StartCompTask
	EndCompTask
)

func (s SagaMessageType) String() string {
	switch s {
	case StartSaga:
		return "Start Saga"
	case EndSaga:
		return "End Saga"
	case AbortSaga:
		return "Abort Saga"
	case StartTask:
		return "Start Task"
	case EndTask:
		return "End Task"
	case StartCompTask:
		return "Start Comp Task"
	case EndCompTask:
		return "End Comp Task"
	default:
		return "unknown"
	}
}

//
// Data Structure representation of a entry in the SagaLog.
// Different SagaMessageTypes utilize different fields.
// Factory Methods are supplied for creation of Saga Messages
// and should be used instead of directly creatinga  sagaMessage struct
//
type SagaMessage struct {
	sagaId  string
	msgType SagaMessageType
	data    []byte
	taskId  string
}

func (sm SagaMessage) GetSagaId() string {
	return sm.sagaId
}

func (sm SagaMessage) GetMessageType() SagaMessageType {
	return sm.msgType
}

func (sm SagaMessage) GetData() []byte {
	return sm.data
}

func (sm SagaMessage) GetTaskId() string {
	return sm.taskId
}

//
// StartSaga SagaMessageType
//  - sagaId - id of the Saga
//  - data - data needed to execute the saga
//
func MakeStartSagaMessage(sagaId string, job []byte) SagaMessage {
	return SagaMessage{
		sagaId:  sagaId,
		msgType: StartSaga,
		data:    job,
	}
}

//
// EndSaga SagaMessageType
//  - sagaId - id of the Saga
//
func MakeEndSagaMessage(sagaId string) SagaMessage {
	return SagaMessage{
		sagaId:  sagaId,
		msgType: EndSaga,
	}
}

//
// AbortSaga SagaMessageType
//  - sagaId - id of the Saga
//
func MakeAbortSagaMessage(sagaId string) SagaMessage {
	return SagaMessage{
		sagaId:  sagaId,
		msgType: AbortSaga,
	}
}

//
// StartTask SagaMessageType
//  - sagaId - id of the Saga
//  - taskId - id of the started Task
//  - data   - data that is persisted to the log, useful for
//             diagnostic information
//
func MakeStartTaskMessage(sagaId string, taskId string, data []byte) SagaMessage {
	return SagaMessage{
		sagaId:  sagaId,
		msgType: StartTask,
		taskId:  taskId,
		data:    data,
	}
}

//
// EndTask SagaMessageType
//  - sagaId - id of the Saga
//  - taskId - id of the completed Task
//  - data - any results from task completion
//
func MakeEndTaskMessage(sagaId string, taskId string, results []byte) SagaMessage {
	return SagaMessage{
		sagaId:  sagaId,
		msgType: EndTask,
		taskId:  taskId,
		data:    results,
	}
}

//
// StartCompTask SagaMessageType
//  - sagaId - id of the Saga
//  - taskId - id of the started compensating task.  Should
//             be the same as the original taskId
//  - data   - data that is persisted to the log, useful for
//             diagnostic information
//
func MakeStartCompTaskMessage(sagaId string, taskId string, data []byte) SagaMessage {
	return SagaMessage{
		sagaId:  sagaId,
		msgType: StartCompTask,
		taskId:  taskId,
		data:    data,
	}
}

//
// EndCompTask SagaMessageType
//  - sagaId - id of the Saga
//  - taskId - id of the completed compensating task.  Should
//             be the same as the original taskId
//  - data - any results from compensating task completion
//
func MakeEndCompTaskMessage(sagaId string, taskId string, results []byte) SagaMessage {
	return SagaMessage{
		sagaId:  sagaId,
		msgType: EndCompTask,
		taskId:  taskId,
		data:    results,
	}
}
