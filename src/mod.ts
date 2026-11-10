const I32_MAX = 2 ** 31 - 1

export abstract class WorkerRunnerError extends Error {}

export class IoError extends WorkerRunnerError {
	override readonly name = this.constructor.name

	constructor(message = '') {
		super(message)
	}
}

export class TerminatedError extends WorkerRunnerError {
	override readonly name = this.constructor.name

	constructor(message = '') {
		super(message)
	}
}

type Options = {
	maxWorkers: number
	idleTimeoutMs: number
	cullStale: boolean
}

type WorkerSlot = {
	worker: Omit<Worker, 'terminate'>
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

export class WorkerRunner<Input, Output> {
	#createWorker: () => Worker
	#options: Options

	#slots: WorkerSlot[] = []
	#id = 0

	constructor(createWorker: () => Worker, options: Options) {
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

	async call(input: Input): Promise<Result<Output, WorkerRunnerError>> {
		const id = this.#id++

		let slot: WorkerSlot
		if (this.#slots.length < this.#options.maxWorkers) {
			slot = this.#addWorkerSlot()
		} else if (this.#options.cullStale) {
			this.#cullStale()
			slot = this.#addWorkerSlot()
		} else {
			while (true) {
				slot = await this.#nextIdle()
				if (slot.state === 'idle') break
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

		const { worker } = slot
		const kind = 'io'
		worker.postMessage({ id, kind, input })

		const ac = new AbortController()
		const { signal } = ac

		return new Promise<Result<Output, WorkerRunnerError>>((res) => {
			slot.terminated.then((reason) => {
				res({ ok: false, error: new TerminatedError(reason) })
				ac.abort()
			})

			worker.addEventListener('error', (e) => {
				res({ ok: false, error: new IoError(e.message) })
				ac.abort()
			}, { signal })

			worker.addEventListener('messageerror', () => {
				res({ ok: false, error: new IoError('Message error') })
				ac.abort()
			}, { signal })

			worker.addEventListener('message', (e) => {
				if (e.data.kind === 'io' && e.data.id === id) {
					clearTimeout(slot.idleTimeout ?? -1)
					slot.idleTimeout = setTimeoutI32Safe(() => {
						slot.terminate('Idle timeout')
					}, this.#options.idleTimeoutMs)
					slot.idle.resolve(slot)
					slot.idle = Promise.withResolvers()
					slot.state = 'idle'

					res({ ok: true, value: e.data.output })
					ac.abort()
				}
			}, { signal })
		})
	}

	#cullStale() {
		let i = 0
		const slots = this.#slots.filter((x) => x.state === 'busy')
		for (const slot of slots) {
			// make sure not to terminate latest (assume most recent)
			if (i++ % 2 === slots.length % 2) {
				slot.terminate('Removed stale')
			}
		}
	}

	#nextIdle(): Promise<WorkerSlot> {
		return Promise.race(this.#slots.map((slot) => slot.idle.promise))
	}

	#addWorkerSlot(): WorkerSlot {
		const worker = this.#createWorker()
		const terminated = Promise.withResolvers<string>()

		const slot: WorkerSlot = {
			worker,
			idle: Promise.withResolvers(),
			terminate: (reason) => {
				worker.terminate()
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
