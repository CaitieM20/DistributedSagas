package sagas

import (
  "fmt"
  "github.com/leanovate/gopter"
  "github.com/leanovate/gopter/gen"
  "github.com/leanovate/gopter/prop"
  "strings"
  "testing"
)

func TestSagaState_ValidateSagaId(t *testing.T) {
  err := validateSagaId("")
  if err == nil {
    t.Error("Invalid Saga Id Should Return Error")
  }

  // validate the correct error is returned
  _, sErrorOk := err.(InvalidSagaMessageError)
  if !sErrorOk {
    t.Error("Expected Returned Error to be InvalidSagaMessageError")
  }
}

func TestSagaState_ValidateTaskId(t *testing.T) {
  err := validateTaskId("")
  if err == nil {
    t.Error(fmt.Sprintf("Invalid Task Id Should Return Error"))
  }

  // validate the correct error is returned
  _, sErrorOk := err.(InvalidSagaMessageError)
  if !sErrorOk {
    t.Error("Expected Returned Error to be InvalidSagaMessageError")
  }
}

func Test_ValidateSagaUpdate(t *testing.T) {
  parameters := gopter.DefaultTestParameters()
  parameters.MinSuccessfulTests = 1000
  properties := gopter.NewProperties(parameters)

  properties.Property("StartSaga message is never valid on an already started saga", prop.ForAll(
    func(state *SagaState, data []byte) bool {
      msg := MakeStartSagaMessage(state.sagaId, data)
      err := validateSagaUpdate(state, msg)

      return err != nil
    },
    GenSagaState(),
    gen.SliceOf(gen.UInt8()),
  ))

  // EndSaga messages are valid if a saga has not been Aborted and all StartTask have EndTask messages
  // If a saga has been aborted all StartTask messages must have corresponding StartCompTask / EndCompTask messages
  // for an EndSaga message to be valid.
  properties.Property("EndSaga message is Valid or returns an error", prop.ForAll(
    func(state *SagaState) bool {

      msg := MakeEndSagaMessage(state.sagaId)
      err := validateSagaUpdate(state, msg)

      validTransition := true
      for _, id := range state.getTaskIds() {

        // if aborted all comp tasks must be completed for all tasks started
        // if not aborted all started tasks must be completed
        if state.isSagaAborted() {
          if !(state.isTaskStarted(id) &&
            state.isCompTaskStarted(id) &&
            state.isCompTaskCompleted(id)) {
            validTransition = false
          }
        } else {
          if !(state.isTaskStarted(id) &&
            state.isTaskCompleted(id)) {
            validTransition = false
          }
        }
      }

      // validate the correct error is returned
      _, sErrorOk := err.(InvalidSagaStateError)

      // either we made a valid transition and had a valid update or applying
      // this message is an invalidTransition and an error was returned.
      validUpdate := validTransition && err == nil
      errorReturned := !validTransition && err != nil && sErrorOk 

      return validUpdate || errorReturned
    },
    GenSagaState(),
  ))

  // Abort messages are valid unless a Saga has been Completed
  properties.Property("AbortSaga message is valid or returns an error", prop.ForAll(
    func(state *SagaState) bool {
      validTransition := !state.isSagaCompleted()

      msg := MakeAbortSagaMessage(state.sagaId)
      err := validateSagaUpdate(state, msg)

      // validate the correct error is returned
      _, sErrorOk := err.(InvalidSagaStateError)

      // either we made a valid transition and had a valid update or applying
      // this message is an invalidTransition and an error was returned.
      validUpdate := validTransition && err == nil 
      errorReturned := !validTransition && err != nil && sErrorOk 

      return validUpdate || errorReturned
    },
    GenSagaState(),
  ))

  // StartTask messages are valid unless a Saga has been Completed or Aborted
  properties.Property("StartTask message is valid or returns an Error", prop.ForAll(

    func(pair StateTaskPair, data []byte) bool {

      state := pair.state
      taskId := pair.taskId

      validTransition := !state.isSagaCompleted() && !state.isSagaAborted() && !state.isTaskCompleted(taskId)

      msg := MakeStartTaskMessage(state.sagaId, taskId, data)
      err := validateSagaUpdate(state, msg)

      // validate the correct error is returned
      _, sErrorOk := err.(InvalidSagaStateError)

      // either we made a valid transition and had a valid update or applying
      // this message is an invalidTransition and an error was returned.
      validUpdate := validTransition && err == nil 
      errorReturned := !validTransition && err != nil && sErrorOk 

      return validUpdate || errorReturned
    },
    GenSagaStateAndTaskId(),
    gen.SliceOf(gen.UInt8()),
  ))

  // EndTask messages are valid if there is a corresponding StartTask message and a Saga
  // has not been aborted or completed
  properties.Property("EndTask message is valid or returns an Error", prop.ForAll(
    func(pair StateTaskPair, data []byte) bool {

      state := pair.state
      taskId := pair.taskId

      validTransition := !state.isSagaCompleted() && !state.isSagaAborted() &&
        state.isTaskStarted(taskId)

      msg := MakeEndTaskMessage(state.sagaId, taskId, data)
      err := validateSagaUpdate(state, msg)

      // validate the correct error is returned
      _, sErrorOk := err.(InvalidSagaStateError)

      // either we made a valid transition and had a valid update or applying
      // this message is an invalidTransition and an error was returned.
      validUpdate := validTransition && err == nil 
      errorReturned := !validTransition && err != nil && sErrorOk 

      return validUpdate || errorReturned
    },
    GenSagaStateAndTaskId(),
    gen.SliceOf(gen.UInt8()),
  ))

  properties.Property("StartCompTask message is valid or returns an Error", prop.ForAll(
    func(pair StateTaskPair, data []byte) bool {

      state := pair.state
      taskId := pair.taskId

      validTransition := state.isSagaAborted() && !state.isSagaCompleted() &&
        state.isTaskStarted(taskId) && !state.isCompTaskCompleted(taskId)

      msg := MakeStartCompTaskMessage(state.sagaId, taskId, data)
      err := validateSagaUpdate(state, msg)

      // validate the correct error is returned
      _, sErrorOk := err.(InvalidSagaStateError)

      // either we made a valid transition and had a valid update or applying
      // this message is an invalidTransition and an error was returned.
      validUpdate := validTransition && err == nil 
      errorReturned := !validTransition && err != nil && sErrorOk 

      return validUpdate || errorReturned
    },
    GenSagaStateAndTaskId(),
    gen.SliceOf(gen.UInt8()),
  ))

  properties.Property("EndCompTask message is valid or returns an Error", prop.ForAll(
    func(pair StateTaskPair, data []byte) bool {

      state := pair.state
      taskId := pair.taskId

      validTransition := state.isSagaAborted() && !state.isSagaCompleted() &&
        state.isTaskStarted(taskId) && state.isCompTaskStarted(taskId)

      msg := MakeEndCompTaskMessage(state.sagaId, taskId, data)
      err := validateSagaUpdate(state, msg)

      // validate the correct error is returned
      _, sErrorOk := err.(InvalidSagaStateError)

      // either we made a valid transition and had a valid update or applying
      // this message is an invalidTransition and an error was returned.
      validUpdate := validTransition && err == nil 
      errorReturned := !validTransition && err != nil && sErrorOk 

      return validUpdate || errorReturned
    },
    GenSagaStateAndTaskId(),
    gen.SliceOf(gen.UInt8()),
  ))

  properties.Property("String method returns correct representation of SagaState", prop.ForAll(
    func(state *SagaState) bool {

      str := state.String()

      if !strings.Contains(str, fmt.Sprintf("SagaId: %v", state.sagaId)) {
        return false
      }

      if state.isSagaAborted() && !strings.Contains(str, "SagaAborted: true") {
        return false
      }

      if !state.isSagaAborted() && !strings.Contains(str, "SagaAborted: false") {
        return false
      }

      if state.isSagaCompleted() && !strings.Contains(str, "SagaCompleted: true") {
        return false
      }

      if !state.isSagaCompleted() && !strings.Contains(str, "SagaCompleted: false") {
        return false
      }

      if len(state.getTaskIds()) > 0 {
        taskSplit := strings.Split(str, "Tasks: [")
        taskString := strings.Split(taskSplit[1], ",")

        for _, taskStr := range taskString {

          split := strings.Split(taskStr, ": ")

          if len(split) >= 2 {
            taskId := strings.TrimSpace(split[0])
            taskStates := strings.Split(split[1], "|")

            for _, taskState := range taskStates {

              ts := strings.TrimSpace(taskState)
              switch ts {
              case "Started":
                if !state.isTaskStarted(taskId) {
                  return false
                }
              case "Completed":
                if !state.isTaskCompleted(taskId) {
                  return false
                }
              case "CompTaskStarted":
                if !state.isCompTaskStarted(taskId) {
                  return false
                }
              case "CompTaskCompleted":
                if !state.isCompTaskCompleted(taskId) {
                  return false
                }
              default:
                fmt.Println(fmt.Sprintf("Unrecognized state taskId: %s, taskState: %s ", taskId, taskState))
                return false
              }
            }
          }
        }
      }

      return true
    },
    GenSagaState(),
  ))

  properties.TestingRun(t)
}