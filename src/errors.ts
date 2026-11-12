export abstract class WorkerRunnerError extends Error {
	override readonly name = this.constructor.name
}

export class WorkerError extends WorkerRunnerError {}
export class WorkerMessageError extends WorkerRunnerError {}
export class StaleInputError extends WorkerRunnerError {}
export class TerminatedError extends WorkerRunnerError {}
