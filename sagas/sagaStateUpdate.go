package sagas

import (
  "fmt"
)

//
// validates that applying the SagaMessage to the supplied SagaState is a valid transition
// and then applies the update if the transition is valid
//
func validateAndUpdateSagaState(state *SagaState, msg SagaMessage) (error) {

  err := validateSagaUpdate(state, msg)
  if err != nil {
    return err
  }

  updateSagaState(state, msg)

  return nil
}

// 
// Checks if applying a SagaMessage to a supplied SagaState is a valid transition.
// Returns nil if its a valid transition
// Returns an error if its an invalid transition
//
func validateSagaUpdate(state *SagaState, msg SagaMessage) (error) {

  if msg.sagaId != state.sagaId {
    return NewInvalidSagaMessageError(fmt.Sprintf("sagaId %s & SagaMessage sagaId %s do not match", state.sagaId, msg.sagaId))
  }

  switch msg.msgType {

  case StartSaga:
    return NewInvalidSagaStateError("Cannot apply a StartSaga Message to an already existing Saga")

  case EndSaga:

    //A Successfully Completed Saga must have StartTask/EndTask pairs for all messages or
    //an aborted Saga must have StartTask/StartCompTask/EndCompTask pairs for all messages
    for taskId := range state.taskState {

      if state.sagaAborted {
        if !(state.isCompTaskStarted(taskId) && state.isCompTaskCompleted(taskId)) {
          return NewInvalidSagaStateError(fmt.Sprintf("End Saga Message cannot be applied to an aborted Saga where Task %s has not completed its compensating Tasks", taskId))
        }
      } else {
        if !state.isTaskCompleted(taskId) {
          return NewInvalidSagaStateError(fmt.Sprintf("End Saga Message cannot be applied to a Saga where Task %s has not completed", taskId))
        }
      }
    }

  case AbortSaga:

    if state.isSagaCompleted() {
      return NewInvalidSagaStateError("AbortSaga Message cannot be applied to a Completed Saga")
    }

  case StartTask:
    err := validateTaskId(msg.taskId)
    if err != nil {
      return err
    }

    if state.isSagaCompleted() {
      return NewInvalidSagaStateError("Cannot StartTask after Saga has been completed")
    }

    if state.isSagaAborted() {
      return NewInvalidSagaStateError("Cannot StartTask after Saga has been aborted")
    }

    if state.isTaskCompleted(msg.taskId) {
      return NewInvalidSagaStateError("Cannot StartTask after it has been completed")
    }

  case EndTask:
    err := validateTaskId(msg.taskId)
    if err != nil {
      return err
    }

    if state.isSagaCompleted() {
      return NewInvalidSagaStateError("Cannot EndTask after Saga has been completed")
    }

    if state.isSagaAborted() {
      return NewInvalidSagaStateError("Cannot EndTask after an Abort Saga Message")
    }

    // All EndTask Messages must have a preceding StartTask Message
    if !state.isTaskStarted(msg.taskId) {
      return NewInvalidSagaStateError(fmt.Sprintf("Cannot have a EndTask Message Before a StartTask Message, taskId: %s", msg.taskId))
    }

  case StartCompTask:
    err := validateTaskId(msg.taskId)
    if err != nil {
      return err
    }

    if state.isSagaCompleted() {
      return NewInvalidSagaStateError("Cannot StartCompTask after Saga has been completed")
    }

    //In order to apply compensating transactions a saga must first be aborted
    if !state.isSagaAborted() {
      return NewInvalidSagaStateError(fmt.Sprintf("Cannot have a StartCompTask Message when Saga has not been Aborted, taskId: %s", msg.taskId))
    }

    // All StartCompTask Messages must have a preceding StartTask Message
    if !state.isTaskStarted(msg.taskId) {
      return NewInvalidSagaStateError(fmt.Sprintf("Cannot have a StartCompTask Message Before a StartTask Message, taskId: %s", msg.taskId))
    }

    if state.isCompTaskCompleted(msg.taskId) {
      return NewInvalidSagaStateError(fmt.Sprintf("Cannot StartCompTask after it has been completed, taskId: %s", msg.taskId))
    }

  case EndCompTask:
    err := validateTaskId(msg.taskId)
    if err != nil {
      return err
    }

    if state.isSagaCompleted() {
      return NewInvalidSagaStateError("Cannot EndCompTask after Saga has been completed")
    }

    //in order to apply compensating transactions a saga must first be aborted
    if !state.isSagaAborted() {
      return NewInvalidSagaStateError(fmt.Sprintf("Cannot have a EndCompTask Message when Saga has not been Aborted, taskId: %s", msg.taskId))
    }

    // All EndCompTask Messages must have a preceding StartTask Message
    if !state.isTaskStarted(msg.taskId) {
      return NewInvalidSagaStateError(fmt.Sprintf("Cannot have a StartCompTask Message Before a StartTask Message, taskId: %s", msg.taskId))
    }

    // All EndCompTask Messages must have a preceding StartCompTask Message
    if !state.isCompTaskStarted(msg.taskId) {
      return NewInvalidSagaStateError(fmt.Sprintf("Cannot have a EndCompTask Message Before a StartCompTaks Message, taskId: %s", msg.taskId))
    }

  }

  return nil
}

//
// Applies the supplied message to the supplied sagaState. Assumes that applying the SagaMessage 
// to the SagaState is a valid transition.  
//
func updateSagaState(state *SagaState, msg SagaMessage) {

  switch msg.msgType {

  case StartSaga:
    panic("Cannot apply a StartSaga Message to an already existing Saga")

  case EndSaga:

    state.sagaCompleted = true

  case AbortSaga:

    state.sagaAborted = true

  case StartTask:

    if msg.data != nil {
      state.addTaskData(msg.taskId, msg.msgType, msg.data)
    }

    state.taskState[msg.taskId] = TaskStarted

  case EndTask:

    state.taskState[msg.taskId] = state.taskState[msg.taskId] | TaskCompleted

    if msg.data != nil {
      state.addTaskData(msg.taskId, msg.msgType, msg.data)
    }

  case StartCompTask:

    state.taskState[msg.taskId] = state.taskState[msg.taskId] | CompTaskStarted

    if msg.data != nil {
      state.addTaskData(msg.taskId, msg.msgType, msg.data)
    }

  case EndCompTask:

    if msg.data != nil {
      state.addTaskData(msg.taskId, msg.msgType, msg.data)
    }

    state.taskState[msg.taskId] = state.taskState[msg.taskId] | CompTaskCompleted

  }
}

//
// Validates that a SagaId Is valid. Returns error if valid, nil otherwise
//
func validateSagaId(sagaId string) error {
  if sagaId == "" {
    return NewInvalidSagaMessageError("sagaId cannot be the empty string")
  } else {
    return nil
  }
}

//
// Validates that a TaskId Is valid. Returns error if valid, nil otherwise
//
func validateTaskId(taskId string) error {
  if taskId == "" {
    return NewInvalidSagaMessageError("taskId cannot be the empty string")
  } else {
    return nil
  }
}