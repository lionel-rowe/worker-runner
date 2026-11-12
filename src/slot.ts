export class WorkerSlot {
	#pool: WorkerSlot[]
	#worker: Promise<Worker>
	#terminate: (value: string | PromiseLike<string>) => void
	terminated: Promise<string>

	idle = Promise.withResolvers<WorkerSlot>()
	idleTimeout: number | ReturnType<typeof setTimeout> | null = null
	state: 'idle' | 'busy' = 'idle'

	constructor(
		pool: WorkerSlot[],
		worker: Promise<Worker>,
		terminated: PromiseWithResolvers<string>,
	) {
		this.#pool = pool
		this.#worker = worker
		this.#terminate = terminated.resolve.bind(terminated)
		this.terminated = terminated.promise
	}

	getWorker(): Promise<Omit<Worker, 'terminate'>> {
		return this.#worker
	}

	async terminate(reason: string) {
		clearTimeout(this.idleTimeout ?? -1)
		void (await this.#worker).terminate()
		this.#terminate(reason)
		const index = this.#pool.indexOf(this)
		if (index !== -1) {
			this.#pool.splice(index, 1)
		}
	}
}
