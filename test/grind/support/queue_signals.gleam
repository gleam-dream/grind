import gleam/erlang/process
import grind/queue
import grind/support/concurrency.{type LeaseCommand}

pub type WorkerProbe {
  WorkerInvoked
  LaterWorkerInvoked
}

pub type RetryPolicyProbe {
  RetryPolicyInvoked(Int, Int)
}

pub type LeaseSignal {
  FirstAttemptStarted(process.Subject(LeaseCommand))
  TakeoverAttemptStarted(process.Subject(LeaseCommand))
}

pub type LongCallEvent {
  LongCallReturned(Result(Bool, queue.ProcessError))
  LongCallDown(process.Down)
}

pub type WorkerDeathSignal {
  WorkerDeathStarted(process.Pid, process.Subject(LeaseCommand))
}

pub type ConcurrentClaimSignal {
  ConcurrentClaimWorkerStarted(process.Subject(LeaseCommand))
}

pub type CapacitySignal {
  CapacityWorkerStarted(Int, process.Subject(LeaseCommand))
}

pub type ConsumerOwnerStart {
  ConsumerOwnerStarted(process.Pid, queue.Consumer, process.Subject(Nil))
  ConsumerOwnerStopCompleted(Result(queue.StopOutcome, queue.StopError))
  ConsumerOwnerFailed(queue.StartError)
}

pub type CoordinatorLossSignal {
  CoordinatorLossStarted(process.Pid, process.Subject(LeaseCommand))
}

pub type OwnerPoolLossSignal {
  OwnerPoolLossStarted(process.Pid, process.Subject(LeaseCommand))
}

pub type OwnerPoolLossOwnerEvent {
  OwnerPoolLossOwnerReady(process.Pid, queue.Consumer)
  OwnerPoolLossOwnerFailed(queue.StartError)
}
