# StageFlow benchmark results

StageFlow 0.13.1, Python 3.14.7, 13th Gen Intel(R) Core(TM) i7-13700KF, Linux-6.18.55-1-MANJARO-x86_64-with-glibc2.44.

Medians of several repeats after a warm-up.

## chain

| nodes | median_s | us_per_node | nodes_per_s |
|---|---|---|---|
| 10 | 9.827e-05 | 8.189 | 122,116 |
| 100 | 0.0006042 | 5.924 | 168,810 |
| 500 | 0.00291 | 5.798 | 172,479 |
| 1000 | 0.005653 | 5.642 | 177,249 |
| 5000 | 0.02988 | 5.973 | 167,423 |

## overhead

| variant | median_s | us_per_node | relative |
|---|---|---|---|
| bare | 0.001173 | 5.805 | 1 |
| stage timeout (the 30 s default) | 0.001604 | 7.941 | 1.368 |
| policy + limits | 0.001793 | 8.875 | 1.529 |
| frame_bytes limit | 0.001496 | 7.404 | 1.275 |
| declared types | 0.00135 | 6.685 | 1.152 |
| debugger (run mode) | 0.001491 | 7.382 | 1.272 |
| Recorder | 0.00145 | 7.176 | 1.236 |
| OpenTelemetry (spans, no exporter) | 0.00691 | 34.2 | 5.893 |

## map

| items | mode | median_s | us_per_item |
|---|---|---|---|
| 10 | sequential | 0.000155 | 15.5 |
| 10 | parallel | 0.0002012 | 20.1 |
| 100 | sequential | 0.0008992 | 8.992 |
| 100 | parallel | 0.001146 | 11.5 |
| 1000 | sequential | 0.008342 | 8.342 |
| 1000 | parallel | 0.01103 | 11.0 |
| 10000 | sequential | 0.08603 | 8.603 |
| 10000 | parallel | 0.1265 | 12.6 |

## concurrency

| items | concurrency | median_s | ideal_s | items_per_s |
|---|---|---|---|---|
| 500 | 1 | 0.7636 | 0.5 | 654.8 |
| 500 | 4 | 0.2175 | 0.125 | 2,298 |
| 500 | 16 | 0.05978 | 0.03125 | 8,365 |
| 500 | 64 | 0.01723 | 0.007812 | 29,023 |
| 500 | unlimited | 0.008323 | 0.001 | 60,077 |

## parallel

| branches | median_s | us_per_branch |
|---|---|---|
| 2 | 8.467e-05 | 42.3 |
| 16 | 0.0002559 | 16.0 |
| 128 | 0.00158 | 12.3 |
| 1024 | 0.01176 | 11.5 |

## frame

| variables | writes | median_s | ns_per_write |
|---|---|---|---|
| 10 | 10000 | 0.002473 | 247.3 |
| 1000 | 10000 | 0.003528 | 352.8 |
| 10000 | 10000 | 0.00402 | 402.0 |
| 100000 | 10000 | 0.004001 | 400.1 |
| 1000000 | 10000 | 0.004667 | 466.7 |

## load

| nodes | from_dict_ms | from_dict_validate_ms | validate_ms | us_per_node |
|---|---|---|---|---|
| 10 | 0.9193 | 0.9413 | 0.02196 | 94.1 |
| 100 | 7.463 | 7.676 | 0.2127 | 76.8 |
| 500 | 36.7 | 37.4 | 0.734 | 74.8 |
| 1000 | 73.7 | 75.2 | 1.517 | 75.2 |
| 5000 | 366.6 | 367.9 | 1.25 | 73.6 |

## cel

| expression | evals | median_s | us_per_eval |
|---|---|---|---|
| comparison | 10000 | 0.01455 | 1.455 |
| macro over 100 | 10000 | 0.2292 | 22.9 |
| string ops | 10000 | 0.02138 | 2.138 |
