import {test} from 'node:test';
import assert from 'node:assert/strict';
import delay from 'delay';
import PQueue from '../source/index.js';

async function getRejection(promise: Promise<unknown>): Promise<unknown> {
	try {
		await promise;
	} catch (error) {
		return error;
	}

	throw new Error('Expected the promise to reject');
}

// Tracks the number of `abort` listeners currently attached to the signal
function createTrackedController() {
	const controller = new AbortController();
	let listenerCount = 0;

	const originalAddEventListener = controller.signal.addEventListener.bind(controller.signal);
	const originalRemoveEventListener = controller.signal.removeEventListener.bind(controller.signal);

	controller.signal.addEventListener = function (type, listener, options) {
		if (type === 'abort') {
			listenerCount++;
		}

		originalAddEventListener(type, listener, options);
	};

	controller.signal.removeEventListener = function (type, listener) {
		if (type === 'abort') {
			listenerCount--;
		}

		originalRemoveEventListener(type, listener);
	};

	return {controller, getListenerCount: () => listenerCount};
}

test('rejects immediately when the signal is already aborted', async () => {
	const queue = new PQueue();
	const controller = new AbortController();
	controller.abort();

	let ran = false;
	const promise = queue.add(() => {
		ran = true;
	}, {signal: controller.signal});

	// The task is never enqueued
	assert.equal(queue.size, 0);
	assert.equal(queue.pending, 0);

	const error = await getRejection(promise);
	assert.equal(error, controller.signal.reason);
	assert.equal((error as Error).name, 'AbortError');

	await delay(10);
	assert.equal(ran, false);
});

test('rejects immediately when the signal is already aborted and the queue is paused', async () => {
	const queue = new PQueue({autoStart: false});
	const controller = new AbortController();
	controller.abort();

	const promise = queue.add(() => 'task', {signal: controller.signal});

	assert.equal(queue.size, 0);

	const error = await getRejection(promise);
	assert.equal(error, controller.signal.reason);
});

test('aborts a task while it is waiting in the queue', async () => {
	const queue = new PQueue({concurrency: 1});
	const controller = new AbortController();

	let ran = false;
	const blocking = queue.add(async () => delay(100));
	const aborted = queue.add(() => {
		ran = true;
	}, {signal: controller.signal});

	assert.equal(queue.size, 1);

	controller.abort();

	// The task is removed from the queue synchronously upon abort
	assert.equal(queue.size, 0);

	// The promise rejects without waiting for the running task to finish
	const error = await getRejection(aborted);
	assert.equal(error, controller.signal.reason);

	await blocking;
	await delay(10);

	// The task never executes later
	assert.equal(ran, false);
	assert.equal(queue.pending, 0);
});

test('queued abort rejects with a custom abort reason', async () => {
	const queue = new PQueue({concurrency: 1});
	const controller = new AbortController();
	const reason = new Error('no longer needed');

	const blocking = queue.add(async () => delay(50));
	const aborted = queue.add(() => 'task', {signal: controller.signal});

	controller.abort(reason);

	const error = await getRejection(aborted);
	assert.equal(error, reason);

	await blocking;
});

test('pre-aborted signal rejects with a custom abort reason', async () => {
	const queue = new PQueue();
	const controller = new AbortController();
	const reason = new Error('gone before arrival');
	controller.abort(reason);

	const error = await getRejection(queue.add(() => 'task', {signal: controller.signal}));
	assert.equal(error, reason);
});

test('aborting a queued task only removes that task when ids are duplicated', async () => {
	const queue = new PQueue({autoStart: false});
	const controller1 = new AbortController();
	const controller2 = new AbortController();

	let survivorRan = false;
	const aborted = queue.add(() => 'first', {id: 'same-id', signal: controller1.signal});
	queue.add(() => {
		survivorRan = true;
		return 'second';
	}, {id: 'same-id', signal: controller2.signal});

	assert.equal(queue.size, 2);

	controller1.abort();

	// Only the task bound to the aborted signal is removed, not every task with the same id
	assert.equal(queue.size, 1);
	const error = await getRejection(aborted);
	assert.equal(error, controller1.signal.reason);

	// The surviving task with the same id is unaffected and still runs
	queue.start();
	await queue.onIdle();
	assert.equal(survivorRan, true);
});

test('aborting a shared signal settles every queued task bound to it', async () => {
	const queue = new PQueue({concurrency: 1});
	const controller = new AbortController();

	const running = queue.add(async ({signal}) => {
		// Running tasks keep the existing signal-passing semantics
		assert.equal(signal, controller.signal);
		await delay(50);
	}, {signal: controller.signal});

	const queued1 = queue.add(() => 'one', {signal: controller.signal});
	const queued2 = queue.add(() => 'two', {signal: controller.signal});

	assert.equal(queue.size, 2);

	controller.abort();

	const [runningError, error1, error2] = await Promise.all([
		getRejection(running),
		getRejection(queued1),
		getRejection(queued2),
	]);

	// Both queued tasks and the running task reject with the abort reason
	assert.equal(error1, controller.signal.reason);
	assert.equal(error2, controller.signal.reason);
	assert.equal(runningError, controller.signal.reason);

	assert.equal(queue.size, 0);
	assert.equal(queue.pending, 0);
});

test('aborting a queued task does not start a paused queue', async () => {
	const queue = new PQueue({autoStart: false});
	const controller = new AbortController();

	let abortedRan = false;
	let otherRan = false;

	const aborted = queue.add(() => {
		abortedRan = true;
	}, {signal: controller.signal});

	queue.add(() => {
		otherRan = true;
	});

	assert.equal(queue.size, 2);

	controller.abort();

	// The aborted task is removed, but the queue stays paused and nothing starts
	assert.equal(queue.size, 1);
	assert.equal(queue.pending, 0);
	assert.equal(queue.isPaused, true);

	const error = await getRejection(aborted);
	assert.equal(error, controller.signal.reason);

	await delay(20);
	assert.equal(abortedRan, false);
	assert.equal(otherRan, false);

	// The remaining task still runs once the queue is started explicitly
	queue.start();
	await queue.onIdle();
	assert.equal(otherRan, true);
});

test('aborting the last queued task emits empty and idle in order', async () => {
	const queue = new PQueue({autoStart: false});
	const controller = new AbortController();

	const events: string[] = [];
	queue.on('add', () => {
		events.push('add');
	});
	queue.on('empty', () => {
		events.push('empty');
	});
	queue.on('idle', () => {
		events.push('idle');
	});

	const aborted = queue.add(() => 'task', {signal: controller.signal});
	assert.deepEqual(events, ['add']);

	controller.abort();

	// Emitted synchronously during abort, after the task is removed from the queue
	assert.deepEqual(events, ['add', 'empty', 'idle']);
	assert.equal(queue.size, 0);

	const error = await getRejection(aborted);
	assert.equal(error, controller.signal.reason);
});

test('abort listener is removed when a queued task is aborted', async () => {
	const queue = new PQueue({autoStart: false});
	const {controller, getListenerCount} = createTrackedController();

	const aborted = queue.add(() => 'task', {signal: controller.signal});
	assert.equal(getListenerCount(), 1);

	controller.abort();
	assert.equal(getListenerCount(), 0);

	await assert.rejects(aborted);
});

test('abort listeners are removed for all queued tasks sharing a signal', async () => {
	const queue = new PQueue({autoStart: false});
	const {controller, getListenerCount} = createTrackedController();

	const promises = [
		queue.add(() => 'one', {signal: controller.signal}),
		queue.add(() => 'two', {signal: controller.signal}),
		queue.add(() => 'three', {signal: controller.signal}),
	];

	assert.equal(getListenerCount(), 3);

	controller.abort();
	assert.equal(getListenerCount(), 0);

	for (const promise of promises) {
		// eslint-disable-next-line no-await-in-loop
		await assert.rejects(promise);
	}
});

test('abort listeners are removed when queued tasks are cleared', async () => {
	const queue = new PQueue({autoStart: false});
	const {controller, getListenerCount} = createTrackedController();

	queue.add(() => 'one', {signal: controller.signal});
	queue.add(() => 'two', {signal: controller.signal});

	assert.equal(getListenerCount(), 2);
	assert.equal(queue.size, 2);

	queue.clear();

	assert.equal(getListenerCount(), 0);
	assert.equal(queue.size, 0);

	// Aborting after clear must not touch the new queue state
	controller.abort();
	assert.equal(queue.size, 0);
	assert.equal(queue.pending, 0);
});

test('abort listener count returns to zero after tasks start and complete', async () => {
	const queue = new PQueue({concurrency: 1});
	const {controller, getListenerCount} = createTrackedController();

	for (let index = 0; index < 3; index++) {
		// eslint-disable-next-line no-await-in-loop
		await queue.add(async () => delay(10), {signal: controller.signal});
		assert.equal(getListenerCount(), 0, `Listener ${index + 1} was not cleaned up`);
	}

	assert.equal(queue.size, 0);
	assert.equal(queue.pending, 0);
});
