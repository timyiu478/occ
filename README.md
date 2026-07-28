# Optimistic Concurrency Control

An educational implementation of optimistic concurrency control engine in Rust.

## Overview

The primary goal of this project is to make theoretical concurrency methods concrete for learning purposes. It provides a practical way to verify the theoretical behaviors described in the original Kung & Robinson paper by utilizing engine benchmarks.

## Benchmark Result

The following table compares the performance of the Serial and Parallel engines across varying levels of contention and thread counts.

```console
=====================================================================================================================
                                     OCC BENCHMARK: SERIAL vs PARALLEL ENGINE                                        
=====================================================================================================================
Engine     | Contention      | Threads | Attempted    | Committed    | Abort Rate  | Throughput/s    | p50 (µs)   | p99 (µs)  
---------------------------------------------------------------------------------------------------------------------
Serial     | Low (100k keys) | 1       | 869310       | 869310       | 0.00 % | 433161.88       | 2.00       | 4.00      
Parallel   | Low (100k keys) | 1       | 922370       | 922370       | 0.00 % | 459418.77       | 2.00       | 3.00      
---------------------------------------------------------------------------------------------------------------------
Serial     | Low (100k keys) | 4       | 225141       | 225129       | 0.01 % | 109366.01       | 7.00       | 239.00    
Parallel   | Low (100k keys) | 4       | 650386       | 650356       | 0.00 % | 324555.12       | 6.00       | 52.00     
---------------------------------------------------------------------------------------------------------------------
Serial     | Low (100k keys) | 8       | 353953       | 353891       | 0.02 % | 176385.75       | 4.00       | 290.00    
Parallel   | Low (100k keys) | 8       | 676520       | 676446       | 0.01 % | 337524.68       | 6.00       | 168.00    
---------------------------------------------------------------------------------------------------------------------
Serial     | Low (100k keys) | 16      | 340725       | 340582       | 0.04 % | 169625.28       | 4.00       | 1543.00   
Parallel   | Low (100k keys) | 16      | 454294       | 454195       | 0.02 % | 226275.81       | 6.00       | 601.00    
---------------------------------------------------------------------------------------------------------------------
Serial     | Med (1k keys)   | 1       | 1163464      | 1163464      | 0.00 % | 579304.23       | 1.00       | 3.00      
Parallel   | Med (1k keys)   | 1       | 1050123      | 1050123      | 0.00 % | 522829.17       | 1.00       | 3.00      
---------------------------------------------------------------------------------------------------------------------
Serial     | Med (1k keys)   | 4       | 247263       | 245708       | 0.63 % | 122710.88       | 25.00      | 125.00    
Parallel   | Med (1k keys)   | 4       | 680283       | 675742       | 0.67 % | 337376.73       | 6.00       | 50.00     
---------------------------------------------------------------------------------------------------------------------
Serial     | Med (1k keys)   | 8       | 315171       | 310061       | 1.62 % | 154611.97       | 38.00      | 229.00    
Parallel   | Med (1k keys)   | 8       | 701227       | 695149       | 0.87 % | 346741.75       | 4.00       | 151.00    
---------------------------------------------------------------------------------------------------------------------
Serial     | Med (1k keys)   | 16      | 387823       | 382744       | 1.31 % | 190558.25       | 3.00       | 956.00    
Parallel   | Med (1k keys)   | 16      | 516498       | 508714       | 1.51 % | 253438.50       | 5.00       | 606.00    
---------------------------------------------------------------------------------------------------------------------
Serial     | High (10 keys)  | 1       | 1195873      | 1195873      | 0.00 % | 595941.33       | 1.00       | 3.00      
Parallel   | High (10 keys)  | 1       | 976362       | 976362       | 0.00 % | 487506.19       | 1.00       | 3.00      
---------------------------------------------------------------------------------------------------------------------
Serial     | High (10 keys)  | 4       | 304801       | 227499       | 25.36 % | 113676.62       | 20.00      | 107.00    
Parallel   | High (10 keys)  | 4       | 670733       | 461085       | 31.26 % | 230298.36       | 6.00       | 51.00     
---------------------------------------------------------------------------------------------------------------------
Serial     | High (10 keys)  | 8       | 384518       | 250359       | 34.89 % | 124871.50       | 24.00      | 220.00    
Parallel   | High (10 keys)  | 8       | 772361       | 523452       | 32.23 % | 261225.01       | 5.00       | 129.00    
---------------------------------------------------------------------------------------------------------------------
Serial     | High (10 keys)  | 16      | 347895       | 184180       | 47.06 % | 91707.92        | 56.00      | 478.00    
Parallel   | High (10 keys)  | 16      | 649126       | 407567       | 37.21 % | 203167.79       | 9.00       | 479.00    
---------------------------------------------------------------------------------------------------------------------
```

**Key Findings:**

The benchmark data highlights the fundamental trade-offs in Optimistic Concurrency Control (OCC).

* The Baseline Cost of Concurrency Management:
At a single thread, the engines exhibit very similar behavior, but the Serial
engine slightly outperforms the Parallel engine under Medium and High
contention (e.g., 595k tx/s vs 487k tx/s at High contention). This highlights
the baseline overhead the Parallel engine incurs to manage active validation
sets and Rule 3 intersection checks, which the Serial engine avoids entirely.

* Scalability and Tail Latency (Low/Medium Contention):
The Parallel engine significantly outperforms the Serial engine as thread
counts increase, achieving nearly 3x the throughput of the Serial engine at
4 and 8 threads under Medium contention. Furthermore, the new latency metrics
reveal massive tail latency spikes in the Serial engine. For instance, at 16
threads under Low contention, the Serial engine maintains a low median latency
(p50 = 4 µs), but its p99 latency explodes to 1543 µs. The Parallel engine
handles this much better, keeping its p99 at 601 µs.

* A Reversal in High Contention Performance:
At 16 threads, the Parallel engine achieves over double the throughput (203k tx/s vs 91k tx/s)
and maintains a notably lower abort rate (37.21% vs 47.06%). While both engines
suffer identical p99 tail latencies (~479 µs) in this worst-case scenario, the
Parallel engine's validation logic resolves conflicts much more efficiently
than the Serial engine's bottlenecked architecture.



**Runtime Specification:**

* CPU: 2.3 GHz Quad-Core Intel Core i5
* Memory: 8 GB 2133 MHz LPDDR3
* OS: macOS 15.3.1

**How to run benchmark:**

```
cargo run --release --example benchmark
```

## References

1. [Kung, Hsiang-Tsung, and John T. Robinson. "On optimistic methods for concurrency control." ACM Transactions on Database Systems (TODS) 6.2 (1981): 213-226.](https://www.eecs.harvard.edu/~htk/publication/1981-tods-kung-robinson.pdf)
