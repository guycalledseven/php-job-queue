<?php

declare(strict_types=1);

namespace guycalledseven\JobQueue;

use Closure;

interface BatchableQueueInterface
{
    /** Atomically fetch the next available job. */
    public function fetchNext(): ?array;

    /** Mark a job as successfully completed. */
    public function markSuccess(string $id): void;

    /** Mark a job as failed with an error message. */
    public function markFailure(string $id, string $error): void;

    /**
     * Insert or update a single job's data.
     * This is the primitive used by the batch importer.
     * Heavy Mongo vibes. :)
     */
    public function upsert(string $id, array $coreData, array $extrasData, bool $updateCore): void;

    /**
     * Executes a callback containing multiple queue operations in the most
     * efficient way for the engine (e.g., inside a single transaction).
     */
    public function performBatch(\Closure $callback);

    /**
     * Fetches all jobs from the queue for exporting.
     *
     * @return iterable<array> An iterable list of jobs, where each job is an
     *                         associative array combining core fields and extras.
     */
    public function fetchAllForExport(): iterable;

    /**
     * Returns the total number of jobs in the queue.
     */
    public function totalItems(): int;

    /**
     * Returns an array with a summary of the queue's progress.
     * The array should contain keys: 'total', 'done', 'errors', 'in_progress', 'remaining'.
     */
    public function progress(): array;

}
