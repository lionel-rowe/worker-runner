import { assertEquals } from '@std/assert'
import type { WorkerRunnerError } from './mod.ts'
import { StaleInputError, WorkerRunner } from './mod.ts'

type Input = { input: string; timeout: number }

function testWorkerFn() {
	// deno-lint-ignore no-explicit-any
	const self = globalThis as any
	self.addEventListener('message', (e: { data: Input }) => {
		const { input, timeout } = e.data

		// blocking wait
		const start = Date.now()
		while (Date.now() - start < timeout);

		self.postMessage(input.toUpperCase())
	})
}

function createWorker() {
	return new Worker(
		`data:text/javascript,${encodeURIComponent(`(${testWorkerFn})()`)}`,
		{ type: 'module' },
	)
}

Deno.test(WorkerRunner.name, async (t) => {
	await t.step('basic functionality', async () => {
		const runner = new WorkerRunner<Input, string>(createWorker, {
			maxWorkers: 2,
			idleTimeoutMs: 100,
			skipStale: false,
		})

		await assertResultsEqual(
			runner,
			[
				['a', 'A'],
				['b', 'B'],
				['c', 'C'],
				['d', 'D'],
			],
		)
	})

	const SKIP_STALE = { skipStale: true }

	await t.step(Object.keys(SKIP_STALE)[0]!, async () => {
		const runner = new WorkerRunner<Input, string>(createWorker, {
			maxWorkers: 2,
			idleTimeoutMs: 100,
			...SKIP_STALE,
		})

		await assertResultsEqual(
			runner,
			[
				// `a` ocupies worker 1/2
				[{ delay: 0, input: 'a', timeout: 100 }, 'A'],
				// `b` occupies worker 2/2
				[{ delay: 0, input: 'b', timeout: 100 }, 'B'],
				// `c` is invalidated as stale by `d` before `a` or `b` can complete
				[{ delay: 0, input: 'c', timeout: 0 }, StaleInputError],
				// workers become ready before `e` starts, so `d` is not marked stale
				[{ delay: 0, input: 'd', timeout: 0 }, 'D'],
				// `e` ocupies worker 1/2
				[{ delay: 110, input: 'e', timeout: 100 }, 'E'],
				// `f` occupies worker 2/2
				[{ delay: 0, input: 'f', timeout: 100 }, 'F'],
				// `g` is invalidated as stale by `h` before `e` or `f` can complete
				[{ delay: 0, input: 'g', timeout: 0 }, StaleInputError],
				// last input in the sequence always completes
				[{ delay: 0, input: 'h', timeout: 0 }, 'H'],
			],
		)
	})
})

type TestConfig = {
	delay: number
	input: string
	timeout: number
}

async function assertResultsEqual(
	runner: WorkerRunner<Input, string>,
	tests: [
		input: string | TestConfig,
		expected: string | typeof WorkerRunnerError,
	][],
) {
	const inputs = tests.map((t) => t[0])
	const expected = tests.map((t) => t[1])

	const promises: Promise<typeof expected[number]>[] = []

	for (const input of inputs) {
		const config: TestConfig = typeof input === 'string' ? { delay: 0, input, timeout: 0 } : input

		await new Promise((res) => setTimeout(res, config.delay))
		const promise = runner.call(config)

		promises.push((async () => {
			const val = await promise
			return val.ok ? val.value : val.error.constructor as typeof WorkerRunnerError
		})())
	}

	assertEquals(await Promise.all(promises), expected)
}
