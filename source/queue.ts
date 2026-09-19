export type RunFunction = () => Promise<unknown>;

export type Queue<Element, Options> = {
	size: number;
	filter: (options: Readonly<Partial<Options>>) => Element[];
	dequeue: () => Element | undefined;
	enqueue: (run: Element, options?: Partial<Options>) => void;
	setPriority: (id: string, priority: number) => void;

	/**
	Remove a specific enqueued element. Implementations must match by element identity, not by `id`, so that tasks sharing the same `id` are not removed by mistake. Returns `true` when the element was found and removed.

	Optional: when a custom queue does not implement this, an aborted queued task still rejects immediately, but its entry is only discarded once it would have been dequeued.
	*/
	remove?: (element: Element) => boolean;
};
