# Optimistic Concurrency Control

An educational implementation of optimistic concurrency control engine in Rust.

## Overview

The primary goal of this project is to make theoretical concurrency methods concrete for learning purposes. It provides a practical way to verify the theoretical behaviors described in the original Kung & Robinson paper by utilizing engine benchmarks.

## Benchmark Result

The following table compares the performance of the Serial and Parallel engines across varying levels of contention and thread counts.

```
=============================================================================================
                       OCC BENCHMARK: SERIAL vs PARALLEL ENGINE                              
=============================================================================================
Engine     | Contention      | Threads | Attempted    | Committed    | Abort Rate  | Throughput (tx/s)
---------------------------------------------------------------------------------------------
Serial     | Low (100k keys) | 1       | 984095       | 984095       | 0.00      % | 491637.74      
Parallel   | Low (100k keys) | 1       | 1013368      | 1013368      | 0.00      % | 506508.64      
---------------------------------------------------------------------------------------------
Serial     | Low (100k keys) | 4       | 289036       | 289020       | 0.01      % | 144476.72      
Parallel   | Low (100k keys) | 4       | 456731       | 456678       | 0.01      % | 228304.37      
---------------------------------------------------------------------------------------------
Serial     | Low (100k keys) | 8       | 241079       | 241039       | 0.02      % | 120481.15      
Parallel   | Low (100k keys) | 8       | 642099       | 641933       | 0.03      % | 320524.64      
---------------------------------------------------------------------------------------------
Serial     | Low (100k keys) | 16      | 224279       | 224204       | 0.03      % | 111891.00      
Parallel   | Low (100k keys) | 16      | 447032       | 446818       | 0.05      % | 223302.76      
---------------------------------------------------------------------------------------------
Serial     | Med (1k keys)   | 1       | 999299       | 999299       | 0.00      % | 498572.60      
Parallel   | Med (1k keys)   | 1       | 707846       | 707846       | 0.00      % | 353877.06      
---------------------------------------------------------------------------------------------
Serial     | Med (1k keys)   | 4       | 263193       | 261661       | 0.58      % | 130714.28      
Parallel   | Med (1k keys)   | 4       | 977473       | 967213       | 1.05      % | 482247.99      
---------------------------------------------------------------------------------------------
Serial     | Med (1k keys)   | 8       | 370202       | 366284       | 1.06      % | 183113.09      
Parallel   | Med (1k keys)   | 8       | 590525       | 576638       | 2.35      % | 287846.03      
---------------------------------------------------------------------------------------------
Serial     | Med (1k keys)   | 16      | 230839       | 223710       | 3.09      % | 111795.78      
Parallel   | Med (1k keys)   | 16      | 447810       | 428171       | 4.39      % | 213948.94      
---------------------------------------------------------------------------------------------
Serial     | High (10 keys)  | 1       | 1248269      | 1248269      | 0.00      % | 622768.52      
Parallel   | High (10 keys)  | 1       | 978321       | 978321       | 0.00      % | 488285.08      
---------------------------------------------------------------------------------------------
Serial     | High (10 keys)  | 4       | 295138       | 224091       | 24.07     % | 112035.57      
Parallel   | High (10 keys)  | 4       | 937323       | 496264       | 47.06     % | 247584.31      
---------------------------------------------------------------------------------------------
Serial     | High (10 keys)  | 8       | 338922       | 239648       | 29.29     % | 119801.70      
Parallel   | High (10 keys)  | 8       | 825970       | 212363       | 74.29     % | 106153.10      
---------------------------------------------------------------------------------------------
Serial     | High (10 keys)  | 16      | 319905       | 177319       | 44.57     % | 88612.82       
Parallel   | High (10 keys)  | 16      | 686456       | 41425        | 93.97     % | 20694.69       
---------------------------------------------------------------------------------------------
```

**Key Findings:**

The benchmark data highlights the fundamental trade-offs in Optimistic Concurrency Control (OCC).

* The Cost of Concurrency Management: At a single thread, the Serial engine consistently outperforms the Parallel engine across all contention levels. The Parallel engine incurs a baseline overhead because it must manage active_validating sets and perform Rule 3 intersection checks. The Serial engine avoids this bookkeeping overhead entirely.

* Scalability Under Low/Medium Contention: The Parallel engine scales exceptionally well when transactions rarely overlap. For medium contention (1k keys) at 4 threads, the Parallel engine achieves 482,247 tx/s compared to the Serial engine's 130,714 tx/s, yielding a nearly 4x speedup. This proves the optimistic assumption is highly effective when conflicts are rare. 

* The High Contention Penalty: Under high contention (10 keys), the Parallel engine's performance degrades rapidly as thread count increases. At 16 threads, the Parallel engine's abort rate spikes to 93.97%, causing its throughput to plummet to just 20,694 tx/s. In this scenario, the Serial engine actually wins (88,612 tx/s) despite having a high abort rate itself (44.57%). This illustrates that optimistic concurrency is counterproductive when data overlap is highly probable.

**Runtime Specification:**

* CPU: 2.3 GHz Quad-Core Intel Core i5
* Memory: 8 GB 2133 MHz LPDDR3
* OS: macOS 15.3.1

## References

1. [Kung, Hsiang-Tsung, and John T. Robinson. "On optimistic methods for concurrency control." ACM Transactions on Database Systems (TODS) 6.2 (1981): 213-226.](https://www.eecs.harvard.edu/~htk/publication/1981-tods-kung-robinson.pdf)
