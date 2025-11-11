<?php

namespace guycalledseven\JobQueue;

final class CsvProfile
{
	/** @var array<string,string[]> logical => [aliases...] */
	public array $aliases = []; // e.g. ['id'=>['id','msisdn','number']]
	/** @var array<string,mixed> */
	public array $defaults = ['status' => 'queued'];
	/** @var array<string,callable> */
	public array $transforms = []; // fn($v): mixed
	public bool $passthrough_unknown = true;

	/** export */
	/** @var string[] */
	public array $export_columns = ['id', 'processed', 'in_progress', 'status', 'result', 'updated_at', 'last_error'];
	/** @var array<string,string> logical => label */
	public array $labels = []; // optional pretty headers
}
