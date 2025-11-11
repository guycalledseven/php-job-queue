<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use guycalledseven\JobQueue\SqliteQueue;
use guycalledseven\JobQueue\CsvProfile;
use guycalledseven\JobQueue\CsvImporter;
use guycalledseven\JobQueue\CsvExporter;

$csv = __DIR__ . '/data/simple.csv';
$db = __DIR__ . '/data/simple.csv-queue.sqlite';
$csv_out = __DIR__ . '/data/simple_out.csv';

$q = SqliteQueue::open($db);

// clean up DB file size and stats
$q->dropAndRecreate(true);

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
        sleep(1);
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
