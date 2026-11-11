export abstract class WorkerRunnerError extends Error {}

export class IoError extends WorkerRunnerError {
	override readonly name = this.constructor.name
}

export class IoMessageError extends WorkerRunnerError {
	override readonly name = this.constructor.name
}

export class StaleInputError extends WorkerRunnerError {
	override readonly name = this.constructor.name
}

export class TerminatedError extends WorkerRunnerError {
	override readonly name = this.constructor.name
}
