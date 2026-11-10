import { assertEquals } from '@std/assert'
import type { Result } from './mod.ts'
import { TerminatedError, WorkerRunner, WorkerRunnerError } from './mod.ts'

/** A crap PRNG */
function prng(seed: number) {
	return () => {
		// Take the sine, multiply to shift decimals, and grab the fractional part
		const x = Math.sin(seed++) * 10_000
		return x - Math.floor(x)
	}
}

function testWorkerFn(seed: number) {
	const random = prng(seed)

	// deno-lint-ignore no-explicit-any
	const self = globalThis as any
	// deno-lint-ignore no-explicit-any
	self.addEventListener('message', async (e: any) => {
		const { input, id } = e.data
		await new Promise((res) => setTimeout(res, 2000 * random()))
		self.postMessage({ id, input, output: input.toUpperCase(), kind: 'io' })
	})
}

const seed = 2068369795

class TestWorkerCreator {
	seed: number
	constructor(seed: number) {
		this.seed = seed
		this.createWorker = this.createWorker.bind(this)
	}
	createWorker() {
		return new Worker(
			`data:text/javascript,${encodeURIComponent(`${prng}; (${testWorkerFn})(${this.seed})`)}`,
			{ type: 'module' },
		)
	}
}

Deno.test(WorkerRunner.name, async (t) => {
	await t.step('basic functionality', async () => {
		const { createWorker } = new TestWorkerCreator(seed)

		const runner = new WorkerRunner<string, string>(createWorker, {
			maxWorkers: 2,
			idleTimeoutMs: 100,
			cullStale: false,
		})

		await assertResultsEqual(
			runner,
			['abc', 'def', 'ghi', 'xyz'],
			[
				{ ok: true, value: 'ABC' },
				{ ok: true, value: 'DEF' },
				{ ok: true, value: 'GHI' },
				{ ok: true, value: 'XYZ' },
			],
		)
	})

	await t.step('cullStale', async () => {
		const { createWorker } = new TestWorkerCreator(seed)

		const runner = new WorkerRunner<string, string>(createWorker, {
			maxWorkers: 2,
			idleTimeoutMs: 100,
			cullStale: true,
		})

		await assertResultsEqual(
			runner,
			['abc', 'def', 'ghi'],
			[
				{ ok: false, error: new TerminatedError('Removed stale') },
				{ ok: true, value: 'DEF' },
				{ ok: true, value: 'GHI' },
			],
		)

		await assertResultsEqual(
			runner,
			['abc', 'def', 'ghi', 'xyz'],
			[
				{ ok: false, error: new TerminatedError('Removed stale') },
				{ ok: false, error: new TerminatedError('Removed stale') },
				{ ok: true, value: 'GHI' },
				{ ok: true, value: 'XYZ' },
			],
		)
	})
})

async function assertResultsEqual<I, O>(
	runner: WorkerRunner<I, O>,
	inputs: I[],
	expected: Result<O, WorkerRunnerError>[],
) {
	const actual = await Promise.all(inputs.map((input) => runner.call(input)))
	assertEquals(actual.length, expected.length)

	for (let i = 0; i < actual.length; ++i) {
		const a = actual[i]!
		const e = expected[i]!

		if (a.ok && e.ok) {
			assertEquals(a.value, e.value)
		} else if (!a.ok && !e.ok) {
			assertEquals(a.error.constructor, e.error.constructor)
			assertEquals(a.error.message, e.error.message)
		} else {
			assertEquals(a, e)
		}
	}
}
