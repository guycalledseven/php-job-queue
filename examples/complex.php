<?php

/**
 *
 * complex test case. after executing php -q complex.php this should be output:
 * Processing 1006 / 6
 * Done 4 / 6
 *
 * WHY? Bacause:
 *
 * Job 1001: The CSV says done: 1. Transform converts this to processed = 1.
 * Status: Already Done. Will be skipped by the loop.
 * Job 1002: The CSV says done: true. Transform converts this to processed = 1.
 * Status: Already Done. Will be skipped by the loop.
 * Job 1003: The CSV has done: (empty) but working: yes. Transform converts this to processed = 0 and in_progress = 1.
 * Status: In Progress. The queue assumes another process is handling it. Will be skipped by the loop.
 * Job 1004: The CSV says state: error.
 * Status: Error. Will be skipped by the loop.
 * Job 1005: The CSV says done: yes. Transform converts this to processed = 1.
 * Status: Already Done. Will be skipped by the loop.
 * Job 1006: All status-related columns are empty. The database sets its defaults.
 * Status: Queued (processed = 0, in_progress = 0). This is the only job that is eligible for processing.
 *
 * final:
 * Job 1001: Yes (was done on import)
 * Job 1002: Yes (was done on import)
 * Job 1003: No (still in progress)
 * Job 1004: No (is an error)
 * Job 1005: Yes (was done on import)
 * Job 1006: Yes (was processed by the loop)
 */
declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use guycalledseven\JobQueue\SqliteQueue;
use guycalledseven\JobQueue\CsvProfile;
use guycalledseven\JobQueue\CsvImporter;
use guycalledseven\JobQueue\CsvExporter;

$csv =  __DIR__ . '/data/complex.csv';
$db =  __DIR__ . '/data/complex-queue.sqlite';
$csv_out =  __DIR__ . '/data/complex_out.csv';

$q = SqliteQueue::open($db);

$importer = new CsvImporter($q);
$exporter = new CsvExporter($q);

$q->dropAndRecreate(true); // clean up DB file size and stats
// $q->vacuum(); // just optimize

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
    'processed'   => fn ($v) => ($v === '' || $v === null) ? null : (int)!!in_array(strtolower((string)$v), ['1', 'true', 'yes', 'ok'], true),
    'in_progress' => fn ($v) => ($v === '' || $v === null) ? null : (int)!!in_array(strtolower((string)$v), ['1', 'true', 'yes'], true),
    'result'      => fn ($v) => ($v === '' || $v === null) ? null : (int)!!in_array(strtolower((string)$v), ['1', 'true', 'yes', 'ok'], true),
    'status'      => fn ($v) => $v === null ? 'queued' : strtolower((string)$v),
];

// import with flexible headers and passthrough extras
$importer->import($csv, $profileIn, ';', true);


$total = $q->totalItems();


// Loop until empty
while ($job = $q->fetchNext()) {
    $id = $job['id'];
    echo("Processing $id / $total\n");
    try {
        // sleep(1);
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

/**
 * different types of export
 */

// Minimal progress report - Only queue state.
$out = new CsvProfile();
$out->export_columns = ['id','status','result','updated_at'];
$out->labels = [
  'id' => 'ID','status' => 'Status','result' => 'Result','updated_at' => 'Updated At'
];
$exporter->export($csv_out . '_report.csv', $out, ';');

// Ops audit - Include errors and processing flags.
$out = new CsvProfile();
$out->export_columns = ['id','processed','in_progress','status','result','updated_at','last_error'];
$exporter->export($csv_out . '_audit.csv', $out, ';');

// Business handoff - State + selected extras mapped from extras JSON.
$out = new CsvProfile();
$out->export_columns = ['id','status','result','updated_at','segment','note'];
$out->labels = ['segment' => 'Segment','note' => 'Note'];
$exporter->export($csv_out . '_handoff.csv', $out, ';');

// Merge with original input - Echo input extras plus final state.
$out = new CsvProfile();
$out->export_columns = ['id','segment','note','status','result','updated_at','last_error'];
$exporter->export($csv_out . '_merge.csv', $out, ';');


// Full dump - Everything core, then your chosen extras.
$profileOut = new CsvProfile();
$profileOut->export_columns = [
  'id','processed','in_progress','status','result','updated_at','last_error',
  // extras you want to expose:
  'segment','note','batch','crm_id'
];
$exporter->export($csv_out, $profileOut, ';');
