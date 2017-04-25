package sagas


// InvalidRequestError should be returned by the SagaLog
// when the request is invalid and the same request will
// fail on restart, equivalent to an HTTP 400
type InvalidRequestError struct {
  s string
}

func (e InvalidRequestError) Error() string {
  return e.s
}

func NewInvalidRequestError(msg string) error {
  return InvalidRequestError{
    s: msg,
  }
}

// InternalLogError should be returned by the SagaLog
// when the request failed, but may succeed on retry
// this is equivalent to an HTTP 500
type InternalLogError struct {
  s string
}

func (e InternalLogError) Error() string {
  return e.s
}

func NewInternalLogError(msg string) error {
  return InternalLogError{
    s: msg,
  }
}

// InvalidSagaStateError signifies that an illegal SagaState would 
// occur from the requested action. 
type InvalidSagaStateError struct {
  s string
}

func (e InvalidSagaStateError) Error() string {
  return e.s
}

func NewInvalidSagaStateError(msg string) error {
  return InvalidSagaStateError{
    s: msg,
  }
}

// InvalidSagaMessageError signifies that the SagaMessage was not valid
type InvalidSagaMessageError struct {
  s string
}

func (e InvalidSagaMessageError) Error() string {
  return e.s
}

func NewInvalidSagaMessageError(msg string) error {
  return InvalidSagaMessageError{
    s: msg,
  }
}

// Checks the error returned by updating saga state.
// Returns true if the error is a FatalErr.
// Returns false if the error is transient and a retry might succeed
func FatalErr(err error) bool {

  switch err.(type) {
  // InvalidSagaState is an unrecoverable error. This indicates a fatal bug in the code
  // which is asking for an impossible transition.
  case InvalidSagaStateError:
    return true

  // InvalidSagaMessage is an unrecoverable error.  This indicates a fatal bug in the code
  // which is applying invalid parameters to a saga.
  case InvalidSagaMessageError:
    return true

  // InvalidRequestError is an unrecoverable error.  This indicates a fatal bug in the code
  // where the SagaLog cannot durably store messages
  case InvalidRequestError:
    return true

  // InternalLogError is a transient error experienced by the log.  It was not
  // able to durably store the message but it is ok to retry.
  case InternalLogError:
    return false

  // unknown error, default to retryable.
  default:
    return true
  }
}