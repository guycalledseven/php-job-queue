<?php

namespace guycalledseven\JobQueue;

use RuntimeException;
use InvalidArgumentException;

final class CsvQueue implements BatchableQueueInterface
{
    private string $path;
    private string $delimiter;
    /** @var string[] */
    private array $header = [];
    /** @var array<int, array<string,string>> */
    private array $rows = [];
    /** @var array<string,int> id => row index */
    private array $idIndex = [];

    private bool $batchInProgress = false;

    /** @var string[] */
    private static array $required = [
        'id','processed','in_progress','status','result','updated_at','last_error'
    ];

    private function __construct(string $path, string $delimiter)
    {
        $this->path = $path;
        $this->delimiter = $delimiter;
    }

    /**
     * Opens an existing queue file or creates a new one.
     * Consistent with SqliteQueue
     *
     * @param string $path
     * @param string $delimiter
     * @return self
     */
    public static function open(string $path, string $delimiter = ';'): self
    {
        if (!is_file($path)) {
            // This block contains the logic that used to be in load()
            $fp = fopen($path, 'c+');
            if (!$fp) {
                throw new RuntimeException("Cannot create queue file: $path");
            }
            // Write the header only if the file is brand new
            if (filesize($path) === 0) {
                fwrite($fp, implode($delimiter, self::$required) . "\n");
            }
            fclose($fp);
        }

        // After ensuring the file exists, we can safely load it.
        return self::load($path, $delimiter);
    }

    /**
     * Loads an EXISTING queue from a CSV file.
     * Strict and will fail if the file does not exist.
     *
     * @param string $path
     * @param string $delimiter
     * @return self
     * @throws RuntimeException if the file is not found or not readable.
     */
    public static function load(string $path, string $delimiter = ';'): self
    {
        // Add a strict check to ensure the file exists before proceeding.
        if (!is_file($path) || !is_readable($path)) {
            throw new RuntimeException("Queue file not found or is not readable: $path");
        }

        // The rest of the method is the same as before
        $q = new self($path, $delimiter);
        $q->readAll();
        $q->ensureColumns(self::$required);
        $q->rebuildIndex();
        if ($q->resetInProgressOnLoad() > 0) {
            $q->save();
        }
        return $q;
    }    

    /** 
     * FIFO: first eligible row. Marks it in_progress=1 and persists. 
     */
    public function fetchNext(): ?array
    {
        foreach ($this->rows as $i => $row) {
            $proc   = isset($row['processed']) ? trim((string)$row['processed']) : '0';
            $prog   = isset($row['in_progress']) ? trim((string)$row['in_progress']) : '0';
            $status = isset($row['status']) ? strtolower(trim((string)$row['status'])) : '';

            $eligible =
                ($proc === '0' || $proc === '') &&
                ($prog === '0' || $prog === '') &&
                ($status === '' || $status === 'queued');

            if ($eligible) {
                $this->rows[$i]['processed']   = '0';
                $this->rows[$i]['in_progress'] = '1';
                $this->rows[$i]['status']      = 'in_progress';
                $this->rows[$i]['updated_at']  = $this->now();
                $this->save();
                return $this->rows[$i];
            }
        }
        return null;
    }

    /** 
     * Mark success for id. 
     */
    public function markSuccess(string $id): void
    {
        $this->updateById($id, [
            'processed'   => '1',
            'in_progress' => '0',
            'status'      => 'ok',
            'result'      => '1',
            'updated_at'  => $this->now(),
            'last_error'  => '',
        ]);
    }

    /** 
     * Mark failure for id. Will be skipped on next runs (status=error). 
     */
    public function markFailure(string $id, string $error): void
    {
        $error = str_replace(["\r","\n"], ' ', $error);
        $this->updateById($id, [
            'processed'   => '0',
            'in_progress' => '0',
            'status'      => 'error',
            'result'      => '0',
            'updated_at'  => $this->now(),
            'last_error'  => $error,
        ]);
    }

    /** 
     * Update row by id with given fields. Unknown fields extend schema. Persists. 
     */
    public function updateById(string $id, array $fields): void
    {
        $id = $this->clean($id);

        if (!isset($this->idIndex[$id])) {
            foreach ($this->idIndex as $k => $idx) {
                if ($this->clean($k) === $id) {
                    $this->idIndex[$id] = $idx;
                    break;
                }
            }
            if (!isset($this->idIndex[$id])) {
                throw new InvalidArgumentException("Unknown id: $id");
            }
        }

        $this->ensureColumns(array_keys($fields));
        $rowIdx = $this->idIndex[$id];

        foreach ($fields as $k => $v) {
            $this->rows[$rowIdx][$k] = $this->scalar($v);
        }
        $this->save();
    }

    /** 
     * Reset all rows back to queued state. Use only when you want a full rerun. 
     */
    public function resetAll(): void
    {
        foreach ($this->rows as $i => $_) {
            $this->rows[$i]['processed']   = '0';
            $this->rows[$i]['in_progress'] = '0';
            $this->rows[$i]['status']      = 'queued';
            $this->rows[$i]['result']      = '';
            $this->rows[$i]['updated_at']  = '';
            $this->rows[$i]['last_error']  = '';
        }
        $this->save();
    }

    /** 
     * Optional: clear stuck in_progress older than N minutes (manual use). 
     */
    public function recoverStaleInProgress(int $minutes = 60): int
    {
        $cut = time() - ($minutes * 60);
        $n = 0;
        foreach ($this->rows as $i => $row) {
            if (($row['in_progress'] ?? '0') === '1') {
                $ts = strtotime($row['updated_at'] ?? '') ?: 0;
                if ($ts < $cut) {
                    $this->rows[$i]['in_progress'] = '0';
                    if (($row['status'] ?? '') === 'in_progress') {
                        $this->rows[$i]['status'] = 'queued';
                    }
                    $n++;
                }
            }
        }
        if ($n) {
            $this->save();
        }
        return $n;
    }

    /** 
     * Quick state snapshot for logging. 
     */
    public function debugState(): array
    {
        $out = [];
        foreach ($this->rows as $r) {
            $out[] = [
                'id' => $r['id'],
                'p' => $r['processed'],
                'ip' => $r['in_progress'],
                'st' => $r['status'],
                'res' => $r['result'],
            ];
        }
        return $out;
    }

    /** 
     * Atomic save 
     */
    public function save(): void
    {
        if ($this->batchInProgress) {
            return; // Don't save during a batch operation
        }

        $dir = dirname($this->path);
        $tmp = $dir . DIRECTORY_SEPARATOR . ('.' . basename($this->path) . '.tmp');

        $fp = fopen($tmp, 'wb');
        if (!$fp) {
            throw new RuntimeException("Cannot open temp file: $tmp");
        }

        $this->fputcsvCompat($fp, $this->header);
        foreach ($this->rows as $row) {
            $ordered = [];
            foreach ($this->header as $col) {
                $ordered[] = $row[$col] ?? '';
            }
            $this->fputcsvCompat($fp, $ordered);
        }

        fflush($fp);
        if (function_exists('fsync')) {
            @fsync($fp);
        }
        fclose($fp);

        if (!@rename($tmp, $this->path)) {
            @unlink($this->path);
            if (!@rename($tmp, $this->path)) {
                @unlink($tmp);
                throw new RuntimeException("Cannot replace {$this->path}");
            }
        }
    }

    // ---------- internals ----------

    private function readAll(): void
    {
        $fp = fopen($this->path, 'rb');
        if (!$fp) {
            throw new RuntimeException("Cannot read file: {$this->path}");
        }

        @flock($fp, LOCK_SH);

        // PHP 8.4 requires explicit $escape
        $header = fgetcsv($fp, 0, $this->delimiter, '"', "\\");
        if ($header === false) {
            fclose($fp);
            throw new RuntimeException("Missing header in CSV");
        }

        $this->header = array_map(function ($h) {
            $h = is_string($h) ? $h : '';
            $h = preg_replace('/^\xEF\xBB\xBF/', '', $h);
            return $this->clean($h);
        }, $header);

        $this->rows = [];
        $lineNo = 1;

        while (($row = fgetcsv($fp, 0, $this->delimiter, '"', "\\")) !== false) {
            $lineNo++;

            // skip fully empty lines
            if ($row === [null] || (count($row) === 1 && $this->clean((string)$row[0]) === '')) {
                continue;
            }

            // build assoc
            $assoc = [];
            $n = max(count($this->header), count($row));
            for ($i = 0; $i < $n; $i++) {
                $key = $this->header[$i] ?? "col_$i";
                $val = $row[$i] ?? '';
                $assoc[$key] = $this->clean((string)$val);
            }

            // skip accidental header duplicates
            if (($assoc['id'] ?? '') === '' || $assoc['id'] === 'id') {
                continue;
            }

            // ensure required cols
            foreach (self::$required as $req) {
                if (!array_key_exists($req, $assoc)) {
                    $assoc[$req] = '';
                }
            }

            // normalize only blanks
            $assoc['id']          = (string)$assoc['id'];
            $assoc['processed']   = ($assoc['processed'] === '' ? '0' : (string)$assoc['processed']);
            $assoc['in_progress'] = ($assoc['in_progress'] === '' ? '0' : (string)$assoc['in_progress']);
            // preserve existing status; set to 'queued' only if blank
            $assoc['status']      = ($assoc['status'] === '' ? 'queued' : (string)$assoc['status']);
            $assoc['result']      = ($assoc['result'] === '' ? '' : (string)$assoc['result']);
            $assoc['updated_at']  = (string)($assoc['updated_at'] ?? '');
            $assoc['last_error']  = (string)($assoc['last_error'] ?? '');

            $this->rows[] = $assoc;
        }

        @flock($fp, LOCK_UN);
        fclose($fp);
    }

    /** 
     * Ensure header contains all columns. Fill missing cells with ''. 
     */
    private function ensureColumns(array $cols): void
    {
        $added = false;
        foreach ($cols as $c) {
            if ($c === '' || $c === null) {
                continue;
            }
            if (!in_array($c, $this->header, true)) {
                $this->header[] = $c;
                $added = true;
            }
        }
        if ($added) {
            foreach ($this->rows as $i => $row) {
                foreach ($cols as $c) {
                    if (!array_key_exists($c, $row)) {
                        $this->rows[$i][$c] = '';
                    }
                }
            }
        }
    }

    private function rebuildIndex(): void
    {
        $this->idIndex = [];
        foreach ($this->rows as $i => $row) {
            $id = $this->clean((string)($row['id'] ?? ''));
            if ($id !== '' && !isset($this->idIndex[$id])) {
                $this->idIndex[$id] = $i;
            }
        }
    }

    /** 
     * Clear any in_progress=1 on load. Switch status=in_progress -> queued. Returns count changed. 
     */
    private function resetInProgressOnLoad(): int
    {
        $n = 0;
        foreach ($this->rows as $i => $row) {
            if (($row['in_progress'] ?? '0') === '1') {
                $this->rows[$i]['in_progress'] = '0';
                if (($row['status'] ?? '') === 'in_progress') {
                    $this->rows[$i]['status'] = 'queued';
                }
                $n++;
            }
        }
        return $n;
    }

    private function now(): string
    {
        return date('Y-m-d H:i:s');
    }

    private function scalar($v): string
    {
        if (is_bool($v)) {
            return $v ? '1' : '0';
        }
        if (is_int($v) || is_float($v)) {
            return (string)$v;
        }
        if ($v === null) {
            return '';
        }
        return (string)$v;
    }

    private function fputcsvCompat($fp, array $fields): void
    {
        if (fputcsv($fp, $fields, $this->delimiter, '"', "\\") === false) {
            throw new RuntimeException('fputcsv failed');
        }
    }

    private function clean(string $s): string
    {
        $s = str_replace("\r", '', $s);
        return trim($s, " \t\n\x0B\x0C\x00\xA0");
    }

    public function upsert(string $id, array $coreData, array $extrasData, bool $updateCore): void
    {
        $this->ensureColumns(array_keys($coreData));
        $this->ensureColumns(array_keys($extrasData));

        $existingIndex = $this->idIndex[$id] ?? null;

        if ($existingIndex !== null) { // Update
            $currentExtras = [];
            // Check if the 'extras' key exists before trying to access it.
            if (isset($this->rows[$existingIndex]['extras']) && !empty($this->rows[$existingIndex]['extras'])) {
                $currentExtras = json_decode((string)$this->rows[$existingIndex]['extras'], true) ?: [];
            }

            $mergedExtras = array_replace($currentExtras, $extrasData);
            $this->rows[$existingIndex]['extras'] = json_encode($mergedExtras);

            if ($updateCore) {
                foreach ($coreData as $key => $val) {
                    if ($val !== null) { // Avoid overwriting with nulls from sparse CSVs
                        $this->rows[$existingIndex][$key] = (string)$val;
                    }
                }
            }
        } else { // Insert
            $newRow = array_fill_keys($this->header, '');
            $newRow['id'] = $id;
            $newRow['status'] = 'queued';
            $newRow['extras'] = json_encode($extrasData);

            if ($updateCore) {
                foreach ($coreData as $key => $val) {
                    if ($val !== null) {
                        $newRow[$key] = (string)$val;
                    }
                }
            }
            $this->rows[] = $newRow;
            $this->rebuildIndex();
        }
    }

    public function performBatch(\Closure $callback)
    {
        $this->batchInProgress = true;
        try {
            // Pass the queue instance itself to the callback
            $result = $callback($this);
        } finally {
            $this->batchInProgress = false;
            // The "commit" for the CSV engine is to save the file once.
            $this->save();
        }
        return $result;
    }

    public function fetchAllForExport(): iterable
    {
        foreach ($this->rows as $row) {
            $extras = [];
            // Check if the key exists before accessing it.
            if (isset($row['extras']) && !empty($row['extras'])) {
                $extras = json_decode((string)$row['extras'], true) ?: [];
            }

            // Make a copy of the row data so we don't modify the original in memory
            $rowData = $row;
            unset($rowData['extras']); // Safely remove from the copy

            // Yield a single, merged array for each job
            yield array_replace($extras, $rowData);
        }
    }

    public function totalItems(): int
    {
        return count($this->rows);
    }

    public function progress(): array
    {
        $stats = [
            'total'       => count($this->rows),
            'done'        => 0,
            'errors'      => 0,
            'in_progress' => 0,
            'remaining'   => 0,
        ];

        foreach ($this->rows as $row) {
            if (isset($row['processed']) && (string)$row['processed'] === '1') {
                $stats['done']++;
            }
            if (isset($row['status']) && $row['status'] === 'error') {
                $stats['errors']++;
            }
            if (isset($row['in_progress']) && (string)$row['in_progress'] === '1') {
                $stats['in_progress']++;
            }
        }

        $stats['remaining'] = $stats['total'] - $stats['done'] - $stats['errors'] - $stats['in_progress'];

        // Ensure 'remaining' doesn't go below zero
        if ($stats['remaining'] < 0) {
            $stats['remaining'] = 0;
        }

        return $stats;
    }


}
