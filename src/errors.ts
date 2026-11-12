/** Abstract class for errors thrown by the WorkerRunner */
export abstract class WorkerRunnerError extends Error {
	override readonly name = this.constructor.name
}

/** Error thrown when a worker encounters an error */
export class WorkerError extends WorkerRunnerError {}
/** Error thrown when a message to the worker cannot be processed */
export class WorkerMessageError extends WorkerRunnerError {}
/** Error thrown when an input is discarded as stale */
export class StaleInputError extends WorkerRunnerError {}
/** Error thrown when a worker is terminated */
export class TerminatedError extends WorkerRunnerError {}
