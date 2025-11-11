<?php

declare(strict_types=1);

namespace guycalledseven\JobQueue;

use RuntimeException;

final class CsvExporter
{
    private BatchableQueueInterface $queue;

    // Use the interface for the constructor and property type
    public function __construct(BatchableQueueInterface $queue)
    {
        $this->queue = $queue;
    }

    public function export(string $csvPath, CsvProfile $profile, string $delimiter = ';'): void
    {
        $fh = fopen($csvPath, 'wb');
        if (!$fh) {
            throw new RuntimeException("Cannot write: $csvPath");
        }

        // Header labels
        $hdr = array_map(function ($col) use ($profile) {
            return $profile->labels[$col] ?? $col;
        }, $profile->export_columns);

        fputcsv($fh, $hdr, $delimiter, '"', "\\");

        // Fetch all data from the queue engine in a standard format.
        $allJobs = $this->queue->fetchAllForExport();

        // Iterate and write to the CSV.
        foreach ($allJobs as $job) {
            $line = [];
            foreach ($profile->export_columns as $col) {
                // Look for the column in the combined job data.
                // Use null coalescing for a clean default.
                $line[] = (string)($job[$col] ?? '');
            }
            fputcsv($fh, $line, $delimiter, '"', "\\");
        }

        fclose($fh);
    }
}
