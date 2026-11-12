import { StaleInputError, TerminatedError, WorkerError, WorkerMessageError, WorkerRunnerError } from './errors.ts'
import { WorkerSlot } from './slot.ts'
export * from './errors.ts'

const I32_MAX = 2 ** 31 - 1

export type WorkerRunnerOptions<I, O> = {
	maxWorkers: number
	idleTimeoutMs: number
	skipStale: boolean
	matches?: (input: I, output: O) => boolean
}

type Success<T> = {
	ok: true
	value: T
}

type Failure<E> = {
	ok: false
	error: E
}

export type Result<T, E> =
	| Success<T>
	| Failure<E>

export class WorkerRunner<I, O> {
	#createWorker: () => Worker | Promise<Worker>
	#options: WorkerRunnerOptions<I, O>
	#slots: WorkerSlot[] = []

	#matches(input: I, output: O) {
		return this.#options.matches?.(input, output) ?? true
	}

	constructor(createWorker: () => Worker | Promise<Worker>, options: WorkerRunnerOptions<I, O>) {
		this.#createWorker = createWorker
		this.#options = options
	}

	warmup(numWorkers = 1) {
		for (let i = this.#slots.length; i < numWorkers; ++i) {
			this.#addWorkerSlot()
		}
	}

	async terminateAll() {
		for (const slot of [...this.#slots]) {
			await slot.terminate('Terminate all')
		}
	}

	#i = -1
	async call(input: I): Promise<Result<O, WorkerRunnerError>> {
		const i = ++this.#i

		let slot: WorkerSlot
		while (true) {
			const idleSlot = this.#slots.find((x) => x.state === 'idle')
			if (idleSlot) {
				slot = idleSlot
				break
			}
			if (this.#slots.length < this.#options.maxWorkers) {
				slot = this.#addWorkerSlot()
				break
			}

			const candidate = await this.#nextCandidate()

			if (this.#options.skipStale && i !== this.#i) {
				return {
					ok: false,
					error: new StaleInputError('Discarded stale input'),
				}
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

		return new Promise<Result<O, WorkerRunnerError>>((res) => {
			slot.terminated.then((reason) => {
				res({ ok: false, error: new TerminatedError(reason) })
				ac.abort()
			})

			worker.addEventListener('error', (e) => {
				res({ ok: false, error: new WorkerError(e.message) })
				ac.abort()
				slot.terminate('Worker error')
			}, { signal })

			worker.addEventListener('messageerror', () => {
				res({ ok: false, error: new WorkerMessageError('Message error') })
				ac.abort()
				slot.terminate('Message error')
			}, { signal })

			worker.addEventListener('message', (e) => {
				if (this.#matches(input, e.data)) {
					slot.state = 'idle'
					slot.idle.resolve(slot)
					slot.idle = Promise.withResolvers()

					clearTimeout(slot.idleTimeout ?? -1)
					slot.idleTimeout = setTimeoutI32Safe(() => {
						slot.terminate('Idle timeout')
					}, this.#options.idleTimeoutMs)

					res({ ok: true, value: e.data })
					ac.abort()
				}
			}, { signal })
		})
	}

	#nextCandidate() {
		return Promise.race(this.#slots.map((slot) => slot.idle.promise))
	}

	#addWorkerSlot(): WorkerSlot {
		const slot = new WorkerSlot(
			this.#slots,
			Promise.resolve(this.#createWorker()),
			Promise.withResolvers(),
		)

		this.#slots.push(slot)
		return slot
	}
}

function setTimeoutI32Safe(fn: () => void, ms: number): number {
	return ms > I32_MAX ? -1 : Number(setTimeout(fn, ms))
}
