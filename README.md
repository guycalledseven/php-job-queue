# JobQueue

## Overview

`JobQueue` is a pluggable job queue library with two interchangeable engines: a concurrent, high-performance SQLite backend for scalable workloads, and a pure-CSV backend where the queue itself is the CSV file. Includes CSV import/export adapters with aliases, transforms, and flexible schema mapping.

It allows importing, processing, and exporting structured data/tasks from CSV files while maintaining full state persistence across runs.

Project was created over a weekend because I needed a way to run long reports on a large CSV files where each row included long running tasks that can fail and should be resumable. I wanted zero dependencies and to keep compatiblity for php 7.4 upwards.

### Typical Use Cases

- Persistent batch processing pipeline.
    
- CSV-to-database ingestion with resumable progress.
    
- Scheduled or restartable job execution.
    
- Converting input CSVs to normalized datasets with flexible export mapping.
    
- Maintaining processing metadata (status, errors, timestamps) outside the original CSV.

- You cannot / do not wan't to install any queue server software and bring additioonal dependencies

## Features

- **SQLite-backed queue**
    
    - Atomic fetch, process, and update of jobs.
        
    - Durable state storage across restarts.
        
    - Automatic recovery of `in_progress` rows after interruption.

    - O(1) row updates, WAL + synchronous=NORMAL, single-writer fast path.

- **CSV-backed queue**
    
    - **It uses input CSV as a queue it self**

    - Suitable for huge and slower batches.

    - Correct but I/O heavy for fast jobs (two full writes per job if you mark on fetch and on complete).
        

- **CSV import and export**
    
    - Reads semicolon-delimited files with flexible headers.
        
    - Writes consistent output CSVs with selectable columns.
        
    - Preserves custom or unknown columns as JSON in an `extras` field.
        
- **Profile-driven mapping**
    
    - Customizable header aliases, default values, and transforms.
        
    - Controlled import/export column sets via `CsvProfile`.
        
    - Optional normalization for booleans, statuses, and timestamps.
        
- **Selective synchronization**
    
    - `updateCore` flag allows either full state import or extras-only merge.
        
    - Merges new data with existing records without overwriting unmodified fields.
        
- **Progress tracking**
    
    - Built-in methods for counting queued, processed, and failed jobs.
        
    - Progress and error summaries available through helper functions.
        
- **Database maintenance**
    
    - Vacuum and analyze utilities for compaction.
        
    - Safe deletion or full reset via table drop or file removal.
        
- **Extras handling**
    
    - Arbitrary extra columns are preserved as JSON.
        
    - Automatically merged on repeated imports.
        
    - Fully exportable through profile configuration.
        
- **Single-writer performance**
    
    - Optimized SQLite pragmas (`WAL`, `synchronous=NORMAL`, memory temp store).
        
    - Transactions batched for fast bulk imports.
        

## Components

- **`SqliteQueue`** — core engine for persistent job management. DB lifecycle, fetch, mark, progress, reset. Writes .sqlite files on disk in specified location.
    
- **`CsvQueue`** — CSV-backed engine - uses CSV file as a queue it self. Correct but I/O heavy. For huge and slower jobs.
    
- **`CsvProfile`** — schema mapping for CSV import/export: aliases, transforms, defaults, export columns and labels.
    
- **`CsvImporter`** — imports CSV into the queue via a `CsvProfile`. Optional `updateCore` to restore state.
    
- **`CsvExporter`** — exports queue state to CSV via a `CsvProfile`.
    
- **`BatchableQueueInterface`** — minimal contract both engines implement: `fetchNext()`, `markSuccess()`, `markFailure()`, `resetAll()`, `progress()`.

## Import modes

- `updateCore=false` (default): import only IDs and extras. Core state in DB is preserved. Extras are merged, never wiped.

- `updateCore=true` update core fields only if present in the CSV header; extras are merged. Use this to restore prior runs.


## Install / Autoload

Install: `composer require guycalledseven/job-queue`

Autoload: PSR-4 "guycalledseven\\JobQueue\\": "src/Queue/"

PHP: 7.4+ (I have my reasons) 😅


## Examples

## Engines and swapping

```php
use guycalledseven\JobQueue\{SqliteQueue, CsvQueue, CsvImporter, CsvExporter, CsvProfile};

// SQLite engine (recommended)
$engine = SqliteQueue::open(__DIR__.'/data/queue.sqlite');

// CSV engine (small files only)
$engine = CsvQueue::load(__DIR__.'/data/queue.csv', ';');

// Import/Export adapters bind to the chosen engine
$importer = new CsvImporter($engine);
$exporter = new CsvExporter($engine);
```


## Simple csv example

Example input data:

```csv
id;bla
1;
2;
3;
4;
```

Example output:

```csv
id;Status;result;"Segment data";note
1;ok;1;1;bla
2;ok;1;1;bla
3;ok;1;1;bla
4;ok;1;1;bla
```

Usage:

```php
require __DIR__ . '/../vendor/autoload.php';

use guycalledseven\JobQueue\SqliteQueue;
use guycalledseven\JobQueue\CsvProfile;
use guycalledseven\JobQueue\CsvImporter;
use guycalledseven\JobQueue\CsvExporter;

$csv = './tests/simple.csv';
$db = './tests/' . basename(($csv)) . '-queue.sqlite';

$q = SqliteQueue::open($db);

$importer = new CsvImporter($q);
$exporter = new CsvExporter($q);

// clean up DB file size and stats
$q->dropAndRecreate(true);

// import minimal set from csv
$profileIn = new CsvProfile();
$profileIn->aliases = [
	'id' => ['id'] // only column we expect
];   

$importer->import($csv, $profileIn, ';');

$total = $q->totalItems();

// Loop until empty
while ($job = $q->fetchNext()) {
	$id = $job['id'];
	$this->logger->info("Processing $id");
	try {
		// your long work using $id
		$ok = true;
		// create additional field data
		$q->updateById($id, [
			'segment' => 1,
			'note' => 'bla',
		]);

		// $ok = false; // or true based on your logic
		$ok ? $q->markSuccess($id)
			: $q->markFailure($id, 'Processor returned false');
	} catch (Throwable $e) {
		$q->markFailure($id, $e->getMessage());
	}
}

$p = $q->progress();
$this->logger->info("Done {$p['done']} / {$p['total']}");

// export minimal data set to csv
$profileOut = new CsvProfile();
$profileOut->export_columns = [
	'id',
	'status',
	'result',
	'segment',
	'note' // pulled from extras
];
$profileOut->labels = [ // optional pretty headers
	'status' => 'Status',
	'segment' => 'Segment data'
];

$exporter->export($csv, $profileOut, ';');
```


## Complex example with transforms

Input csv data:

```csv
msisdn;done;working;state;ok;timestamp;error
1001;1;0;ok;1;2025-11-09 09:12:03;
1002;true;no;OK;yes;2025-11-09 09:12:04;temporary failure
1003;;yes;queued;;2025-11-09 09:12:05;
1004;false;;error;0;2025-11-09 09:12:06;api timeout
1005;yes;no;;yes;;network lag
1006;;;;;;  ; extra spaces
```

Output csv:

```csv
id;segment;note;status;result;updated_at;last_error
1001;;;ok;1;"2025-11-09 09:12:03";
1002;;;ok;1;"2025-11-09 09:12:04";"temporary failure"
1003;;;queued;;"2025-11-09 09:12:05";
1004;;;error;0;"2025-11-09 09:12:06";"api timeout"
1005;;;queued;1;;"network lag"
1006;1;bla;ok;1;"2025-11-10 18:55:23";
```

Code

```php
use guycalledseven\JobQueue\{SqliteQueue, CsvImporter, CsvExporter, CsvProfile};

$csv =  __DIR__ . '/data/complex.csv';
$db =  __DIR__ . '/data/complex-queue.sqlite';
$csv_out =  __DIR__ . '/data/complex_out.csv';

$q = SqliteQueue::open($db);

$importer = new CsvImporter($q);
$exporter = new CsvExporter($q);

$q->dropAndRecreate(true); // clean up DB file size and stats
// $q->vacuum(); // just optimize, do not delete

$profileIn = new CsvProfile();
// Header mapping
$profileIn->aliases = [
	'id'          => ['id', 'msisdn', 'number'],
	'processed'   => ['processed', 'done'],
	'in_progress' => ['in_progress', 'working'],
	'status'      => ['status', 'state'],
	'result'      => ['result', 'ok'],
	'updated_at'  => ['updated_at', 'timestamp', 'last_update'],
	'last_error'  => ['last_error', 'error', 'message'],
];
// Value normalization and transformation
$profileIn->transforms = [
	'processed'   => fn($v) => ($v === '' || $v === null) ? null : (int)!!in_array(strtolower((string)$v), ['1', 'true', 'yes', 'ok'], true),
	'in_progress' => fn($v) => ($v === '' || $v === null) ? null : (int)!!in_array(strtolower((string)$v), ['1', 'true', 'yes'], true),
	'result'      => fn($v) => ($v === '' || $v === null) ? null : (int)!!in_array(strtolower((string)$v), ['1', 'true', 'yes', 'ok'], true),
	'status'      => fn($v) => $v === null ? 'queued' : strtolower((string)$v),
];

// import with flexible headers and passthrough extras
$importer->import($csv, $profileIn, ';');

// Loop until empty
while ($job = $q->fetchNext()) {
    $id = $job['id'];
    try {
        // ... processing

        $ok ? $q->markSuccess($id)
            : $q->markFailure($id, 'Processor returned false');
    } catch (Throwable $e) {
        $q->markFailure($id, $e->getMessage());
    }
}



// export selective columns, merging extras by keys requested
$out = new CsvProfile();
$out->export_columns = ['id','segment','note','status','result','updated_at','last_error'];
$exporter->export($csv_out . '_merge.csv', $out, ';');
```



# CsvQueue

The point of this implementation is to keep things simple, without any external dependencies or additional engines (like MySQL, Redis, Rabbit MQ etc.). 

**The CSV file which is being processed is the queue itself.**

## CsvQueue example

```php
// To use the slower, file-based CSV engine:
try {
    $q = CsvQueue::load($csv);
} catch (RuntimeException $e) {
    die("Error: " . $e->getMessage());
}

// $q = CsvQueue::open($csv); // Safe: creates the file if it's the first 

// import minimal set from csv
$profileIn = new CsvProfile();
$profileIn->aliases = [
    'id' => ['id'] // only column we expect
];

$importer = new CsvImporter($q);
$exporter = new CsvExporter($q);

$importer->import($csv, $profileIn, ';');

$total = $q->totalItems();


// Loop until empty
while ($job = $q->fetchNext()) {
    $id = $job['id'];
    echo("Processing $id / $total\n");
    try {
        sleep(3);
        // throw new \Exception('testna greska');
        // your long work using $id
        // $ok = false;
        $ok = true;
        // create additional field data
        $q->updateById($id, [
            'segment' => 1,
            'note' => 'bla',
        ]);

        // $ok = false; // or true based on your logic
        $ok ? $q->markSuccess($id)
            : $q->markFailure($id, 'Processor returned false');
    } catch (Throwable $e) {
        $q->markFailure($id, $e->getMessage());
    }
}

$p = $q->progress();
echo("Done {$p['done']} / {$p['total']}\n");

// export minimal data set to csv
$profileOut = new CsvProfile();
$profileOut->export_columns = [
    'id',
    'status',
    'result',
    'segment',
    'note' // pulled from extras
];
$profileOut->labels = [ // optional pretty headers
    'status' => 'Status',
    'segment' => 'Segment data'
];

$exporter->export($csv_out, $profileOut, ';');
```


## License

MIT License.

