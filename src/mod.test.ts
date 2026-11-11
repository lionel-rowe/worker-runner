import { assertEquals } from '@std/assert'
import { StaleInputError, WorkerRunner, WorkerRunnerError } from './mod.ts'

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
				'a',
				{ input: 'b', timeout: 100 },
				'c',
				'd',
			],
			[
				'A',
				'B',
				'C',
				'D',
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
				{ input: 'a', timeout: 100 },
				'b',
				'c',
				'd',
				'e',
				'f',
			],
			[
				'A',
				'B',
				new StaleInputError('Discarded stale input'),
				new StaleInputError('Discarded stale input'),
				new StaleInputError('Discarded stale input'),
				'F',
			],
		)
	})
})

async function assertResultsEqual(
	runner: WorkerRunner<Input, string>,
	inputs: (string | Input)[],
	expected: (string | WorkerRunnerError)[],
) {
	const actual = await Promise.all(inputs.map(async (x) => {
		const val = await runner.call(
			typeof x === 'string' ? { input: x, timeout: 0 } : x,
		)

		return val.ok ? val.value : val.error
	}))

	for (const x of [...actual, ...expected]) {
		if (Error.isError(x)) {
			x.stack = x.stack?.replace(/(\n\s+at )[^]*/, '$1<truncated trace>')
		}
	}

	assertEquals(actual, expected)
}
