# StageFlow benchmark results

StageFlow 0.13.1, Python 3.14.7, 13th Gen Intel(R) Core(TM) i7-13700KF, Linux-6.18.55-1-MANJARO-x86_64-with-glibc2.44.

Medians of several repeats after a warm-up.

## chain

| nodes | median_s | us_per_node | nodes_per_s |
|---|---|---|---|
| 10 | 9.726e-05 | 8.105 | 123,386 |
| 100 | 0.0006019 | 5.901 | 169,474 |
| 500 | 0.002907 | 5.791 | 172,678 |
| 1000 | 0.005567 | 5.556 | 179,994 |
| 5000 | 0.02901 | 5.799 | 172,434 |

## overhead

| variant | median_s | us_per_node | relative |
|---|---|---|---|
| bare | 0.00115 | 5.693 | 1 |
| stage timeout (the 30 s default) | 0.001626 | 8.051 | 1.414 |
| policy + limits | 0.001802 | 8.923 | 1.567 |
| frame_bytes limit | 0.001521 | 7.531 | 1.323 |
| declared types | 0.001357 | 6.716 | 1.18 |
| debugger (run mode) | 0.001543 | 7.639 | 1.342 |
| Recorder | 0.001455 | 7.203 | 1.265 |
| OpenTelemetry (spans, no exporter) | 0.006969 | 34.5 | 6.061 |

## map

| items | mode | median_s | us_per_item |
|---|---|---|---|
| 10 | sequential | 0.0001619 | 16.2 |
| 10 | parallel | 0.0001997 | 20.0 |
| 100 | sequential | 0.0009673 | 9.673 |
| 100 | parallel | 0.00112 | 11.2 |
| 1000 | sequential | 0.009163 | 9.163 |
| 1000 | parallel | 0.01139 | 11.4 |
| 10000 | sequential | 0.08845 | 8.845 |
| 10000 | parallel | 0.1147 | 11.5 |

## concurrency

| items | concurrency | median_s | ideal_s | items_per_s |
|---|---|---|---|---|
| 500 | 1 | 0.7569 | 0.5 | 660.6 |
| 500 | 4 | 0.2201 | 0.125 | 2,272 |
| 500 | 16 | 0.06827 | 0.03125 | 7,324 |
| 500 | 64 | 0.01699 | 0.007812 | 29,430 |
| 500 | unlimited | 0.008113 | 0.001 | 61,626 |

## parallel

| branches | median_s | us_per_branch |
|---|---|---|
| 2 | 8.766e-05 | 43.8 |
| 16 | 0.0002523 | 15.8 |
| 128 | 0.001602 | 12.5 |
| 1024 | 0.01242 | 12.1 |

## frame

| variables | writes | median_s | ns_per_write |
|---|---|---|---|
| 10 | 10000 | 0.002434 | 243.4 |
| 1000 | 10000 | 0.003418 | 341.8 |
| 10000 | 10000 | 0.003471 | 347.1 |
| 100000 | 10000 | 0.0039 | 390.0 |
| 1000000 | 10000 | 0.004559 | 455.9 |

## load

| nodes | from_dict_ms | from_dict_validate_ms | validate_ms | us_per_node |
|---|---|---|---|---|
| 10 | 0.08307 | 0.1071 | 0.02406 | 10.7 |
| 100 | 0.2252 | 0.3671 | 0.1419 | 3.671 |
| 500 | 0.9196 | 1.587 | 0.6678 | 3.175 |
| 1000 | 1.768 | 3.121 | 1.353 | 3.121 |
| 5000 | 8.871 | 16.0 | 7.112 | 3.197 |

## cel

| expression | evals | median_s | us_per_eval |
|---|---|---|---|
| comparison | 10000 | 0.01464 | 1.464 |
| macro over 100 | 10000 | 0.2294 | 22.9 |
| string ops | 10000 | 0.02133 | 2.133 |

## memory

| case | size | peak_kib | held_kib | bytes_per_node | events_kept |
|---|---|---|---|---|---|
| chain | 100 | 69.1 | 68.2 | 707.4 | 204 |
| chain | 1000 | 659.6 | 659.0 | 675.4 | 2004 |
| chain | 10000 | 3,297 | 3,296 | 337.6 | 10000 |
| chain | 50000 | 3,287 | 3,286 | 67.3 | 10000 |
| sequential map | 100 | 155.3 | 131.7 | 1,590 | 406 |
| sequential map | 1000 | 1,545 | 1,355 | 1,582 | 4006 |
| sequential map | 10000 | 5,874 | 3,762 | 601.4 | 10000 |
| 1 MiB value, 1000 stages | 1000 | 658.5 | 657.9 | 674.3 | 2004 |
