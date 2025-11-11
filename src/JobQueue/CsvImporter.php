<?php

declare(strict_types=1);

namespace guycalledseven\JobQueue;

use RuntimeException;
use InvalidArgumentException;

final class CsvImporter
{
    private BatchableQueueInterface $queue;

    public function __construct(BatchableQueueInterface $queue)
    {
        $this->queue = $queue;
    }

    public function import(string $csvPath, CsvProfile $profile, string $delimiter = ';', bool $updateCore = false): int
    {
        return $this->queue->performBatch(function (BatchableQueueInterface $q) use ($csvPath, $profile, $delimiter, $updateCore) {

            $fh = fopen($csvPath, 'rb');
            if (!$fh) {
                throw new RuntimeException("Cannot open: $csvPath");
            }

            $rawHeader = fgetcsv($fh, 0, $delimiter, '"', "\\");
            if ($rawHeader === false) {
                fclose($fh);
                throw new RuntimeException("Missing header");
            }

            $norm = static function ($s) {
                $s = is_string($s) ? $s : '';
                $s = preg_replace('/^\xEF\xBB\xBF/', '', $s);
                return strtolower(trim($s, " \t\n\x0B\x0C\x00\xA0"));
            };

            $hdr = array_map($norm, $rawHeader);

            // (header to logical mapping logic is unchanged)
            $h2logical = array_fill_keys($hdr, null);
            foreach ($profile->aliases as $logical => $syns) {
                foreach ($syns as $syn) {
                    $key = $norm($syn);
                    if (array_key_exists($key, $h2logical)) {
                        $h2logical[$key] = $logical;
                    }
                }
            }
            if (array_search('id', $h2logical, true) === false) {
                fclose($fh);
                throw new InvalidArgumentException("CSV lacks an 'id' alias");
            }

            $coreFields = ['processed', 'in_progress', 'status', 'result', 'updated_at', 'last_error'];
            $presentLogical = [];
            foreach ($h2logical as $hname => $lname) {
                if ($lname !== null) {
                    $presentLogical[$lname] = true;
                }
            }

            $xform = function ($field, $val) use ($profile) {
                return (isset($profile->transforms[$field]) && is_callable($profile->transforms[$field]))
                    ? ($profile->transforms[$field])($val)
                    : $val;
            };

            $count = 0;

            while (($row = fgetcsv($fh, 0, $delimiter, '"', "\\")) !== false) {
                if ($row === [null]) {
                    continue;
                }

                $cells = [];
                foreach ($hdr as $i => $hname) {
                    // Safely get the value, defaulting to null if the row is too short.
                    // This ignores columns in the row that are beyond the header count.
                    $v = $row[$i] ?? null;
                    $cells[$hname] = $v;
                }

                $logical = [];
                $extras  = [];
                foreach ($cells as $hname => $v) {
                    if ($v !== null) {
                        $v = trim(str_replace("\r", '', (string)$v));
                    }

                    $lname = $h2logical[$hname] ?? null;
                    if ($lname === 'id') {
                        $logical['id'] = $v;
                        continue;
                    }

                    if ($lname !== null && in_array($lname, $coreFields, true)) {
                        $logical[$lname] = $v;
                    } else {
                        $key = $lname ?? $hname;
                        if ($v !== null && $v !== '') {
                            $extras[$key] = $v;
                        }
                    }
                }

                $id = (string)($logical['id'] ?? '');
                if ($id === '' || strtolower($id) === 'id') {
                    continue;
                }

                foreach ($profile->defaults as $k => $def) {
                    $isCore = in_array($k, $coreFields, true);
                    if ($isCore) {
                        if ($updateCore && isset($presentLogical[$k]) && (!isset($logical[$k]) || $logical[$k] === '' || $logical[$k] === null)) {
                            $logical[$k] = $def;
                        }
                    } else {
                        if (!isset($extras[$k]) || $extras[$k] === '' || $extras[$k] === null) {
                            $extras[$k] = $def;
                        }
                    }
                }

                foreach ($logical as $k => $v) {
                    if ($k !== 'id') {
                        $logical[$k] = $xform($k, $v);
                    }
                }
                foreach ($extras as $k => $v) {
                    $extras[$k]  = $xform($k, $v);
                }

                $coreData = $logical;
                unset($coreData['id']);

                $q->upsert($id, $coreData, $extras, $updateCore);
                $count++;
            }

            fclose($fh);
            return $count;
        });
    }

}
