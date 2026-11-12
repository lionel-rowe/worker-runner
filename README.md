# WorkerRunner

Distribute work to a pool of workers, optionally discarding stale workloads when the pool is at capacity.

## Example

```ts
const runner = new WorkerRunner(() => new Worker('worker.js'))
const output = await runner.call(input)
```
