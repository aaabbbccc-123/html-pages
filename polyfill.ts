import { Buffer } from 'buffer';
import * as process from 'process';

window.Buffer = Buffer;
window.process = process;

// Bun's node:process browser polyfill has a bug: drainQueue reads item.array but
// nextTick enqueues items as { fun, args }, so item.array is always undefined.
// Override with a correct implementation.
(window.process as any).nextTick = function nextTick(fun: (...args: any[]) => void, ...args: any[]) {
  queueMicrotask(() => fun(...args));
};
