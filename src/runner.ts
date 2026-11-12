import { StaleInputError, TerminatedError, WorkerError, WorkerMessageError } from './errors.ts'
import { WorkerSlot } from './slot.ts'
import { setTimeoutI32Safe } from './utils.ts'

/** Options for configuring the WorkerRunner instance */
export type WorkerRunnerOptions<I, O> = {
	/** The maximum number of workers to spawn at any one time */
	maxWorkers?: number
	/** Timeout in milliseconds before idle workers are terminated */
	idleTimeoutMs?: number
	/** Whether to invalidate (throw) on stale inputs when the worker pool is at capacity */
	invalidateStale?: boolean
	/**
	 * Determine if a given output matches the input. Supplying this option is only necessary in either of the following
	 * cases:
	 * - The worker posts other types of messages that may not correspond to the input
	 * - The worker does asynchronous processing that may result in outputs arriving out of order
	 *
	 * @param input The input value to match against
	 * @param output The output value to check
	 * @returns Whether the output matches the input
	 */
	matches?(input: I, output: O): boolean
}

/**
 * A runner to manage and distribute work to a pool of workers to process inputs and produce outputs, optionally
 * discarding stale workloads when the pool is at capacity.
 *
 * @example
 * ```ts
 * const runner = new WorkerRunner(() => new Worker('worker.js'))
 * const output = await runner.call(input)
 * ```
 */
export class WorkerRunner<I, O> {
	#createWorker: () => Worker | Promise<Worker>
	#options: Required<WorkerRunnerOptions<I, O>>
	#pool: WorkerSlot[] = []

	constructor(createWorker: () => Worker | Promise<Worker>, options?: WorkerRunnerOptions<I, O>) {
		this.#createWorker = createWorker
		const matches = options?.matches?.bind(options) ?? (() => true)
		this.#options = {
			maxWorkers: 5,
			idleTimeoutMs: 30_000,
			invalidateStale: false,
			...options,
			matches,
		}
	}

	/**
	 * Warm up the worker pool by spawning workers up to the specified number
	 * @param numWorkers The number of workers to spawn
	 */
	warmup(numWorkers = 1) {
		for (let i = this.#pool.length; i < numWorkers; ++i) {
			this.#addWorkerSlot()
		}
	}

	/** Terminate all workers in the pool */
	async terminateAll() {
		for (const slot of [...this.#pool]) {
			await slot.terminate('All workers terminated')
		}
	}

	#i = -1
	/**
	 * Call the worker pool with the given input and return the output
	 * @param input The input value to process
	 * @returns The output produced by the worker
	 * @throws {StaleInputError} If the input is discarded as stale (only when `invalidateStale` option is true)
	 * @throws {TerminatedError} If the worker is terminated
	 * @throws {WorkerError} If the worker encounters an error
	 * @throws {WorkerMessageError} If the message to the worker cannot be processed
	 */
	async call(input: I): Promise<O> {
		const i = ++this.#i

		let slot: WorkerSlot
		while (true) {
			const idleSlot = this.#pool.find((x) => x.state === 'idle')
			if (idleSlot) {
				slot = idleSlot
				break
			}
			if (this.#pool.length < this.#options.maxWorkers) {
				slot = this.#addWorkerSlot()
				break
			}

			const candidate = await this.#nextCandidate()

			if (this.#options.invalidateStale && i !== this.#i) {
				throw new StaleInputError('Discarded stale input')
			}

			if (candidate.state === 'idle') {
				slot = candidate
				break
			}
		}

		clearTimeout(slot.idleTimeout ?? -1)
		slot.idleTimeout = null
		slot.state = 'busy'

		const worker = await slot.getWorker()
		worker.postMessage(input)

		const ac = new AbortController()
		const { signal } = ac

		return new Promise<O>((res, rej) => {
			slot.terminated.then((reason) => {
				rej(new TerminatedError(reason))
				ac.abort()
			})

			worker.addEventListener('error', (e) => {
				rej(new WorkerError(e.message))
				ac.abort()
				slot.terminate('Worker error')
			}, { signal })

			worker.addEventListener('messageerror', () => {
				rej(new WorkerMessageError('Message error'))
				ac.abort()
				slot.terminate('Message error')
			}, { signal })

			worker.addEventListener('message', (e) => {
				if (this.#options.matches(input, e.data)) {
					slot.state = 'idle'
					slot.idle.resolve(slot)
					slot.idle = Promise.withResolvers()

					clearTimeout(slot.idleTimeout ?? -1)
					slot.idleTimeout = setTimeoutI32Safe(() => {
						slot.terminate('Idle timeout')
					}, this.#options.idleTimeoutMs)

					res(e.data)
					ac.abort()
				}
			}, { signal })
		})
	}

	#nextCandidate() {
		return Promise.race(this.#pool.map((slot) => slot.idle.promise))
	}

	#addWorkerSlot(): WorkerSlot {
		const slot = new WorkerSlot(
			this.#pool,
			Promise.resolve(this.#createWorker()),
			Promise.withResolvers(),
		)

		this.#pool.push(slot)
		return slot
	}
}
