<?php

declare(strict_types=1);

namespace guycalledseven\JobQueue;

use PDO;
use RuntimeException;

final class SqliteQueue implements BatchableQueueInterface
{
    /** @var PDO */
    private $pdo;

    private const TABLE = 'queue';
    private const SCHEMA = "
        CREATE TABLE IF NOT EXISTS " . self::TABLE . " (
          id TEXT PRIMARY KEY,
          processed INTEGER NOT NULL DEFAULT 0,      -- 0 or 1
          in_progress INTEGER NOT NULL DEFAULT 0,    -- 0 or 1
          status TEXT NOT NULL DEFAULT 'queued',     -- '', queued, in_progress, ok, error
          updated_at TEXT NULL,                      -- 'YYYY-MM-DD HH:MM:SS'
          last_error TEXT NULL,
          result INTEGER NULL,                       -- NULL, 0, 1
		  extras TEXT NULL							 -- JSON with extras if exists

        );
        CREATE INDEX IF NOT EXISTS idx_queue_scan
          ON " . self::TABLE . "(processed, in_progress, status);
    ";

    private function __construct(PDO $pdo)
    {
        $this->pdo = $pdo;
    }

    /** 
	 * Open or create the SQLite file and initialize schema. Also clears stuck in_progress rows. 
	 */
    public static function open(string $dbPath): self
    {
        $pdo = new PDO('sqlite:' . $dbPath, null, null, [
            PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_EMULATE_PREPARES   => false,
        ]);

        // Performance pragmas suitable for a single-writer CLI job
        $pdo->exec("PRAGMA journal_mode=WAL;");
        $pdo->exec("PRAGMA synchronous=NORMAL;");
        $pdo->exec("PRAGMA temp_store=MEMORY;");
        $pdo->exec("PRAGMA foreign_keys=ON;");

        // Schema
        foreach (array_filter(array_map('trim', explode(';', self::SCHEMA))) as $stmt) {
            if ($stmt !== '') {
                $pdo->exec($stmt . ';');
            }
        }

        $self = new self($pdo);

        // Single worker: clear any half-done rows
        $self->clearInProgressOnStart();

        return $self;
    }

    /** 
	 * Atomically fetch the first eligible job and mark it in_progress. Returns row or null. 
	 */
    public function fetchNext(): ?array
    {
        // BEGIN IMMEDIATE prevents write races if you ever parallelize
        $this->pdo->beginTransaction();

        $id = $this->pdo->query("
            SELECT id
            FROM " . self::TABLE . "
            WHERE processed=0 AND in_progress=0 AND (status='' OR status='queued')
            ORDER BY rowid
            LIMIT 1
        ")->fetchColumn();

        if ($id === false) {
            $this->pdo->commit();
            return null;
        }

        $now = $this->now();
        $upd = $this->pdo->prepare("
            UPDATE " . self::TABLE . "
            SET in_progress=1, status='in_progress', updated_at=:now
            WHERE id=:id AND in_progress=0 AND processed=0
        ");
        $upd->execute([':id' => $id, ':now' => $now]);

        if ($upd->rowCount() !== 1) {
            // Lost a race; nothing to do
            $this->pdo->commit();
            return null;
        }

        $stmt = $this->pdo->prepare("SELECT * FROM " . self::TABLE . " WHERE id=:id");
        $stmt->execute([':id' => $id]);
        $row = $stmt->fetch();

        $this->pdo->commit();
        return $row ?: null;
    }

    public function markSuccess(string $id): void
    {
        $stmt = $this->pdo->prepare("
            UPDATE " . self::TABLE . "
            SET processed=1, in_progress=0, status='ok', result=1,
                updated_at=:now, last_error=NULL
            WHERE id=:id
        ");
        $stmt->execute([':id' => $id, ':now' => $this->now()]);
    }

    public function markFailure(string $id, string $error): void
    {
        $stmt = $this->pdo->prepare("
            UPDATE " . self::TABLE . "
            SET processed=0, in_progress=0, status='error', result=0,
                updated_at=:now, last_error=:err
            WHERE id=:id
        ");
        $stmt->execute([
            ':id'  => $id,
            ':now' => $this->now(),
            ':err' => str_replace(["\r", "\n"], ' ', $error),
        ]);
    }

    /**
     * Update arbitrary allowed fields by id. Use for custom metadata or corrections.
     * Allowed: processed,in_progress,status,result,updated_at,last_error
     */
    public function updateById(string $id, array $fields): void
    {
        // core columns we store as columns
        $allowed = ['processed', 'in_progress', 'status', 'result', 'updated_at', 'last_error', 'extras'];

        // Split into column updates and extras updates
        $extrasDelta = [];
        foreach ($fields as $k => $v) {
            if (!in_array($k, $allowed, true)) {
                $extrasDelta[$k] = $v;
                unset($fields[$k]);
            }
        }

        // If caller passed an 'extras' JSON string/array, normalize and merge with delta
        if (array_key_exists('extras', $fields)) {
            $base = $fields['extras'];
            $baseArr = is_array($base) ? $base : (json_decode((string)$base, true) ?: []);
            $fields['extras'] = $baseArr; // keep as array for now
        } else {
            $fields['extras'] = []; // start empty, maybe merge delta below
        }

        if ($extrasDelta) {
            // Load current extras and merge
            $stmt = $this->pdo->prepare("SELECT extras FROM " . self::TABLE . " WHERE id=:id");
            $stmt->execute([':id' => $id]);
            $cur = $stmt->fetchColumn();
            $curArr = $cur ? (json_decode((string)$cur, true) ?: []) : [];
            $fields['extras'] = array_replace($curArr, (array)$fields['extras'], $extrasDelta);
        }

        // Build SQL
        $set = [];
        $params = [':id' => $id];
        foreach ($fields as $k => $v) {
            if (!in_array($k, $allowed, true)) {
                continue;
            }
            if ($k === 'extras') {
                $params[':extras'] = empty($v) ? null : json_encode($v, JSON_UNESCAPED_UNICODE);
                $set[] = "extras = :extras";
            } else {
                $set[] = "$k = :$k";
                $params[":$k"] = $v;
            }
        }

        if (!$set) {
            return;
        }

        $sql = "UPDATE " . self::TABLE . " SET " . implode(', ', $set) . " WHERE id=:id";
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute($params);
    }


    /** 
	 * Reset all rows to queued state. 
	 */
    public function resetAll(): void
    {
        $this->pdo->exec("
            UPDATE " . self::TABLE . "
            SET processed=0, in_progress=0, status='queued', result=NULL,
                updated_at=NULL, last_error=NULL
        ");
    }

    /** 
	 * Optional: retry all error rows by setting them back to queued. 
	 */
    public function retryErrors(): int
    {
        $stmt = $this->pdo->prepare("
            UPDATE " . self::TABLE . "
            SET status='queued', result=NULL, last_error=NULL
            WHERE processed=0 AND status='error'
        ");
        $stmt->execute();
        return $stmt->rowCount();
    }

    /** 
	 * Optional counters 
	 */
    public function counts(): array
    {
        $q = $this->pdo->query("
            SELECT
              SUM(CASE WHEN processed=1 THEN 1 ELSE 0 END) AS done,
              SUM(CASE WHEN processed=0 AND (status='' OR status='queued') AND in_progress=0 THEN 1 ELSE 0 END) AS queued,
              SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS errors,
              SUM(CASE WHEN in_progress=1 THEN 1 ELSE 0 END) AS in_progress
            FROM " . self::TABLE . "
        ");
        $r = $q->fetch();
        return array_map('intval', $r ?: ['done' => 0, 'queued' => 0, 'errors' => 0, 'in_progress' => 0]);
    }

    public function progress(): array
    {
        $row = $this->pdo->query("
			SELECT
			COUNT(*)                                        AS total,
			SUM(CASE WHEN processed=1 THEN 1 ELSE 0 END)    AS done,
			SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS errors,
			SUM(CASE WHEN in_progress=1 THEN 1 ELSE 0 END)  AS in_progress
			FROM " . self::TABLE . "
		")->fetch(PDO::FETCH_ASSOC);

        $row = array_map('intval', $row);
        $row['remaining'] = $row['total'] - $row['done'] - $row['errors'] - $row['in_progress'];
        return $row;
    }


    public function totalItems(): int
    {
        return (int)$this->pdo->query("SELECT COUNT(*) FROM " . self::TABLE . "")->fetchColumn();
    }

    public function vacuum(bool $truncateWal = true): void
    {
        // If a transaction is open, commit it so VACUUM can run
        if ($this->pdo->inTransaction()) {
            $this->pdo->commit();
        }

        // In WAL mode, checkpoint first so the main DB can be compacted
        $mode = strtolower((string)$this->pdo->query("PRAGMA journal_mode;")->fetchColumn());
        if ($mode === 'wal') {
            $this->pdo->exec(
                $truncateWal
                    ? "PRAGMA wal_checkpoint(TRUNCATE);"
                    : "PRAGMA wal_checkpoint(FULL);"
            );
        }

        // Compact main database and refresh stats
        $this->pdo->exec("VACUUM;");
        $this->pdo->exec("ANALYZE;");   // optional but helpful after big changes
    }

    /**
     * Delete the SQLite database file and its WAL/SHM side files.
     *
     * The method commits any open transaction, closes the PDO handle,
     * and removes the main database file along with its optional WAL and SHM files.
     * It throws a RuntimeException if the database file cannot be found or is already deleted.
     *
     *  usage:
     *  $q = SqliteQueue::open($db);
     *  $q->deleteDatabase();
     *  $q = SqliteQueue::open($db); // recreate from scratch - reconnect needed!!
     */
    public function deleteDatabase(): void
    {
        // Must commit any open transaction first
        if ($this->pdo->inTransaction()) {
            $this->pdo->commit();
        }

        // Determine the physical file path
        $file = $this->pdo->query("PRAGMA database_list;")->fetch(PDO::FETCH_ASSOC)['file'] ?? null;

        if ($file && is_file($file)) {
            // Close the PDO handle explicitly
            $this->pdo = null;

            // Remove WAL and SHM side files if present
            @unlink($file . '-wal');
            @unlink($file . '-shm');
            @unlink($file);
        } else {
            throw new RuntimeException("SQLite database file not found or already deleted.");
        }
    }

    /**
     * Drop the existing queue table and recreate the schema from scratch.
     *
     * This method commits any open transaction, removes the current `queue` table,
     * recreates the schema defined in {@see self::SCHEMA}, and then resets
     * journal metadata for compactness by running `PRAGMA wal_checkpoint`,
     * `VACUUM`, and `ANALYZE`.  It is useful for resetting the queue to a
     * clean state during development or when a full rebuild is required.
     *
     * @throws PDOException if any of the SQL statements fail.
     *
     * @return void
     */
    public function dropAndRecreate(): void
    {
        if ($this->pdo->inTransaction()) {
            $this->pdo->commit();
        }

        // Drop old data table
        $this->pdo->exec("DROP TABLE IF EXISTS queue;");

        // Recreate schema exactly as at startup
        foreach (array_filter(array_map('trim', explode(';', self::SCHEMA))) as $stmt) {
            if ($stmt !== '') {
                $this->pdo->exec($stmt . ';');
            }
        }

        // Reset journal metadata for compactness
        $this->pdo->exec("PRAGMA wal_checkpoint(TRUNCATE);");
        $this->pdo->exec("VACUUM;");
        $this->pdo->exec("ANALYZE;");
    }


    // internals

    private function clearInProgressOnStart(): void
    {
        $this->pdo->exec("
            UPDATE " . self::TABLE . "
            SET in_progress=0,
                status = CASE WHEN status='in_progress' THEN 'queued' ELSE status END
            WHERE in_progress=1
        ");
    }


    private function now(): string
    {
        return date('Y-m-d H:i:s');
    }

    public function setExtras(string $id, array $extras): void
    {
        $this->updateById($id, ['extras' => $extras]);
    }

    public function getExtras(string $id): array
    {
        $stmt = $this->pdo->prepare("SELECT extras FROM " . self::TABLE . " WHERE id=:id");
        $stmt->execute([':id' => $id]);
        $json = $stmt->fetchColumn();
        return $json ? (json_decode((string)$json, true) ?: []) : [];
    }

    /**
     * Executes a callback within a database transaction.
     *
     * @param \Closure $callback The function to execute. It will receive the PDO instance.
     * @return mixed The return value of the callback.
     * @throws \Throwable If the callback throws an exception, the transaction is rolled back.
     */
    public function transaction(\Closure $callback)
    {
        if ($this->pdo->inTransaction()) {
            return $callback($this->pdo);
        }

        $this->pdo->beginTransaction();

        try {
            $result = $callback($this->pdo);
            $this->pdo->commit();
            return $result;
        } catch (\Throwable $e) {
            $this->pdo->rollBack();
            throw $e; // Re-throw the exception
        }
    }

    public function upsert(string $id, array $coreData, array $extrasData, bool $updateCore): void
    {
        $ins = $this->pdo->prepare("INSERT OR IGNORE INTO " . self::TABLE . "(id) VALUES(:id)");
        $ins->execute([':id' => $id]);

        // This part for handling extras is correct.
        if (!empty($extrasData)) {
            $curStmt = $this->pdo->prepare("SELECT extras FROM " . self::TABLE . " WHERE id=:id");
            $curStmt->execute([':id' => $id]);
            $curJson = $curStmt->fetchColumn();
            if ($curJson) {
                $curArr = json_decode((string)$curJson, true) ?: [];
                $extrasData = array_replace($curArr, $extrasData);
            }
        }
        $extrasJson = empty($extrasData) ? null : json_encode($extrasData, JSON_UNESCAPED_UNICODE);

        $params = [':id' => $id];
        $setClauses = [];

        // Always include extras in the update, even if null, to clear it if needed.
        $setClauses[] = 'extras = :extras';
        $params[':extras'] = $extrasJson;

        if ($updateCore) {
            // Check if the value is null before adding it to the UPDATE statement.
            foreach ($coreData as $key => $value) {
                if ($value !== null) {
                    $setClauses[] = "$key = :$key";
                    $params[":$key"] = $value;
                }
            }
        }

        // Only run the UPDATE statement if there is something to update.
        // (We always update extras, so the count will usually be > 0)
        if (!empty($setClauses)) {
            $sql = "UPDATE " . self::TABLE . " SET " . implode(', ', $setClauses) . " WHERE id=:id";
            $upd = $this->pdo->prepare($sql);
            $upd->execute($params);
        }
    }

    public function performBatch(\Closure $callback)
    {
        if ($this->pdo->inTransaction()) {
            return $callback($this);
        }

        $this->pdo->beginTransaction();
        try {
            // We pass the queue instance itself to the callback
            $result = $callback($this);
            $this->pdo->commit();
            return $result;
        } catch (\Throwable $e) {
            $this->pdo->rollBack();
            throw $e;
        }
    }
    public function fetchAllForExport(): iterable
    {
        $stmt = $this->pdo->query("SELECT * FROM " . self::TABLE . " ORDER BY rowid");
        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            $extras = [];
            if (!empty($row['extras'])) {
                $extras = json_decode((string)$row['extras'], true) ?: [];
            }
            unset($row['extras']); // Avoid duplication

            // Yield a single, merged array for each job
            yield array_replace($extras, $row);
        }
    }

}
