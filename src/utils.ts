const I32_MAX = 2 ** 31 - 1

/** Set a timeout if `ms` is within the valid int32 limit, otherwise discard the callback */
export function setTimeoutI32Safe(fn: () => void, ms: number): number {
	return ms > I32_MAX ? -1 : Number(setTimeout(fn, ms))
}
