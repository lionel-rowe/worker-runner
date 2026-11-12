export class WorkerSlot {
	#slots: WorkerSlot[]
	#worker: Promise<Worker>
	#terminate: (value: string | PromiseLike<string>) => void
	terminated: Promise<string>

	idle = Promise.withResolvers<WorkerSlot>()
	idleTimeout: number | ReturnType<typeof setTimeout> | null = null
	state: 'idle' | 'busy' = 'idle'

	constructor(
		slots: WorkerSlot[],
		worker: Promise<Worker>,
		terminated: PromiseWithResolvers<string>,
	) {
		this.#slots = slots
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
		const index = this.#slots.indexOf(this)
		if (index !== -1) {
			this.#slots.splice(index, 1)
		}
	}
}
