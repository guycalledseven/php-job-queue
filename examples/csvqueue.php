<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use guycalledseven\JobQueue\CsvQueue;
use guycalledseven\JobQueue\CsvProfile;
use guycalledseven\JobQueue\CsvImporter;
use guycalledseven\JobQueue\CsvExporter;

$csv = __DIR__ . '/data/csvqueue.csv';
$csv_out = __DIR__ . '/data/csvqueue_out.csv';

// To use the fast, robust SQLite engine:
// $queue = \guycalledseven\JobQueue\SqliteQueue::open('./my-queue.sqlite');

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
