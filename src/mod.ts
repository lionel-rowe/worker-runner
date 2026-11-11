import { IoError, IoMessageError, StaleInputError, TerminatedError, WorkerRunnerError } from './errors.ts'
export * from './errors.ts'

const I32_MAX = 2 ** 31 - 1

export type Options<I, O> = {
	maxWorkers: number
	idleTimeoutMs: number
	skipStale: boolean
	matches?: (input: I, output: O) => boolean
}

type WorkerSlot = {
	worker: Promise<Omit<Worker, 'terminate'>>
	idle: PromiseWithResolvers<WorkerSlot>
	terminate: (reason: string) => void
	terminated: Promise<string>
	idleTimeout: number | null
	state: 'idle' | 'busy'
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
	#options: Options<I, O>
	#slots: WorkerSlot[] = []

	#matches(input: I, output: O) {
		return this.#options.matches?.(input, output) ?? true
	}

	constructor(createWorker: () => Worker | Promise<Worker>, options: Options<I, O>) {
		this.#createWorker = createWorker
		this.#options = options
	}

	warmup(numWorkers = 1) {
		for (let i = 0; i < numWorkers; i++) {
			if (this.#slots.length < this.#options.maxWorkers) {
				this.#addWorkerSlot()
			} else break
		}
	}

	#i = -1

	async call(input: I): Promise<Result<O, WorkerRunnerError>> {
		const i = ++this.#i

		let slot: WorkerSlot
		if (this.#slots.length < this.#options.maxWorkers) {
			slot = this.#addWorkerSlot()
		} else {
			while (true) {
				slot = await this.#nextIdle()
				if (slot.state === 'idle') break
			}

			if (this.#options.skipStale && i !== this.#i) {
				return {
					ok: false,
					error: new StaleInputError('Discarded stale input'),
				}
			}
		}

		// Move the selected slot to the end of the array to mark it as most recently used
		this.#slots.push(
			...this.#slots.splice(
				this.#slots.indexOf(slot),
				1,
			),
		)

		clearTimeout(slot.idleTimeout ?? -1)
		slot.idleTimeout = null
		slot.state = 'busy'

		const worker = await slot.worker
		worker.postMessage(input)

		const ac = new AbortController()
		const { signal } = ac

		return new Promise<Result<O, WorkerRunnerError>>((res) => {
			slot.terminated.then((reason) => {
				res({ ok: false, error: new TerminatedError(reason) })
				ac.abort()
			})

			worker.addEventListener('error', (e) => {
				res({ ok: false, error: new IoError(e.message) })
				ac.abort()
			}, { signal })

			worker.addEventListener('messageerror', () => {
				res({ ok: false, error: new IoMessageError('Message error') })
				ac.abort()
			}, { signal })

			worker.addEventListener('message', (e) => {
				if (this.#matches(input, e.data)) {
					clearTimeout(slot.idleTimeout ?? -1)
					slot.idleTimeout = setTimeoutI32Safe(() => {
						slot.terminate('Idle timeout')
					}, this.#options.idleTimeoutMs)
					slot.idle.resolve(slot)
					slot.idle = Promise.withResolvers()
					slot.state = 'idle'

					res({ ok: true, value: e.data })
					ac.abort()
				}
			}, { signal })
		})
	}

	#nextIdle(): Promise<WorkerSlot> {
		return Promise.race(this.#slots.map((slot) => slot.idle.promise))
	}

	#addWorkerSlot(): WorkerSlot {
		const worker = Promise.resolve(this.#createWorker())
		const terminated = Promise.withResolvers<string>()

		const slot: WorkerSlot = {
			worker,
			idle: Promise.withResolvers(),
			terminate: async (reason) => {
				void (await worker).terminate()
				terminated.resolve(reason)
				const index = this.#slots.indexOf(slot)
				if (index !== -1) {
					this.#slots.splice(index, 1)
				}
			},
			terminated: terminated.promise,
			idleTimeout: null,
			state: 'idle',
		}

		this.#slots.push(slot)

		return slot
	}
}

function setTimeoutI32Safe(fn: () => void, ms: number): number {
	return ms > I32_MAX ? -1 : Number(setTimeout(fn, ms))
}
