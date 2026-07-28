use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use occ::{OccEngine, ParallelEngine, SerialEngine};

// =========================================================================
// Benchmark Configuration Types
// =========================================================================

#[derive(Clone, Copy, Debug)]
pub enum ContentionLevel {
    Low,    // 100,000 keys (rare collisions)
    Medium, // 1,000 keys (moderate collisions)
    High,   // 10 keys (heavy hot-spot contention)
}

impl ContentionLevel {
    fn key_space_size(&self) -> usize {
        match self {
            ContentionLevel::Low => 100_000,
            ContentionLevel::Medium => 1_000,
            ContentionLevel::High => 10,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            ContentionLevel::Low => "Low (100k keys)",
            ContentionLevel::Medium => "Med (1k keys)",
            ContentionLevel::High => "High (10 keys)",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct WorkloadConfig {
    pub num_threads: usize,
    pub contention: ContentionLevel,
    pub write_ratio: f64, // e.g. 0.2 = 80% Reads, 20% Writes
    pub keys_per_tx: usize,
    pub test_duration: Duration,
}

#[derive(Debug)]
pub struct BenchmarkResult {
    pub engine_name: &'static str,
    pub config: WorkloadConfig,
    pub total_attempts: u64,
    pub successful_commits: u64,
    pub aborts: u64,
    pub elapsed_secs: f64,
    pub p50_latency_us: f64,
    pub p99_latency_us: f64,
}

impl BenchmarkResult {
    pub fn throughput(&self) -> f64 {
        self.successful_commits as f64 / self.elapsed_secs
    }

    pub fn abort_rate(&self) -> f64 {
        if self.total_attempts == 0 {
            0.0
        } else {
            (self.aborts as f64 / self.total_attempts as f64) * 100.0
        }
    }
}

// Lightweight thread-local random number generator (zero dependencies)
struct FastRng(u64);
impl FastRng {
    fn new(seed: u64) -> Self {
        Self(if seed == 0 { 0xdeadbeef } else { seed })
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn gen_range(&mut self, max: usize) -> usize {
        (self.next_u64() as usize) % max
    }
    fn gen_bool(&mut self, probability: f64) -> bool {
        let val = (self.next_u64() % 10_000) as f64 / 10_000.0;
        val < probability
    }
}

// =========================================================================
// Core Benchmark Runner Function
// =========================================================================
pub fn run_benchmark<'a, E>(
    engine_name: &'static str,
    engine: &'a E,
    config: WorkloadConfig,
) -> BenchmarkResult
where
    E: OccEngine<'a, usize, usize> + Sync + Send + 'a,
{
    let running = Arc::new(AtomicBool::new(true));
    let total_attempts = Arc::new(AtomicU64::new(0));
    let successful_commits = Arc::new(AtomicU64::new(0));
    let aborts = Arc::new(AtomicU64::new(0));

    let start_time = Instant::now();

    // std::thread::scope allows threads to safely borrow `engine` without 'static bounds
    let mut all_latencies = thread::scope(|s| {
        let mut handles = Vec::with_capacity(config.num_threads);
        
        for thread_id in 0..config.num_threads {
            let running_clone = Arc::clone(&running);
            let total_clone = Arc::clone(&total_attempts);
            let success_clone = Arc::clone(&successful_commits);
            let aborts_clone = Arc::clone(&aborts);

            let handle = s.spawn(move || {
                let mut rng = FastRng::new((thread_id as u64 + 1) * 987654321);
                let key_space = config.contention.key_space_size();
                
                // Pre-allocate vector to prevent resize overheads in the hot loop
                let mut local_latencies = Vec::with_capacity(500_000); 

                while running_clone.load(Ordering::Relaxed) {
                    total_clone.fetch_add(1, Ordering::Relaxed);

                    let tx_start = Instant::now();

                    let res = engine.transaction(|tx| {
                        for _ in 0..config.keys_per_tx {
                            let key = rng.gen_range(key_space);
                            if rng.gen_bool(config.write_ratio) {
                                let val = rng.gen_range(1000);
                                tx.write(key, val);
                            } else {
                                let _ = tx.read(&key);
                            }
                        }
                        Ok(())
                    });

                    // Record latency in microseconds
                    local_latencies.push(tx_start.elapsed().as_micros() as u32);

                    match res {
                        Ok(_) => {
                            success_clone.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            aborts_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                local_latencies
            });
            handles.push(handle);
        }

        // Run benchmark for the configured duration while worker threads execute
        thread::sleep(config.test_duration);
        running.store(false, Ordering::SeqCst);
        
        // Collect all latency vectors from joined threads
        let mut merged_latencies = Vec::new();
        for handle in handles {
            if let Ok(mut thread_latencies) = handle.join() {
                merged_latencies.append(&mut thread_latencies);
            }
        }
        merged_latencies
    });

    let elapsed = start_time.elapsed().as_secs_f64();

    // Calculate Percentiles
    let mut p50_latency = 0.0;
    let mut p99_latency = 0.0;
    
    if !all_latencies.is_empty() {
        all_latencies.sort_unstable(); // Sort to find percentiles
        let len = all_latencies.len() as f64;
        let p50_idx = (len * 0.50) as usize;
        let p99_idx = (len * 0.99) as usize;
        
        p50_latency = all_latencies[p50_idx.min(all_latencies.len() - 1)] as f64;
        p99_latency = all_latencies[p99_idx.min(all_latencies.len() - 1)] as f64;
    }

    BenchmarkResult {
        engine_name,
        config,
        total_attempts: total_attempts.load(Ordering::SeqCst),
        successful_commits: successful_commits.load(Ordering::SeqCst),
        aborts: aborts.load(Ordering::SeqCst),
        elapsed_secs: elapsed,
        p50_latency_us: p50_latency,
        p99_latency_us: p99_latency,
    }
}

// =========================================================================
// Main Driver & Report Generator
// =========================================================================

fn main() {
    println!(
        "====================================================================================================================="
    );
    println!(
        "                                     OCC BENCHMARK: SERIAL vs PARALLEL ENGINE                                        "
    );
    println!(
        "====================================================================================================================="
    );

    let durations = Duration::from_secs(2);
    let keys_per_tx = 4;

    // Test combinations
    let thread_counts = vec![1, 4, 8, 16];
    let contentions = vec![
        ContentionLevel::Low,
        ContentionLevel::Medium,
        ContentionLevel::High,
    ];

    println!(
        "{:<10} | {:<15} | {:<7} | {:<12} | {:<12} | {:<11} | {:<15} | {:<10} | {:<10}",
        "Engine",
        "Contention",
        "Threads",
        "Attempted",
        "Committed",
        "Abort Rate",
        "Throughput/s",
        "p50 (µs)",
        "p99 (µs)"
    );
    println!(
        "---------------------------------------------------------------------------------------------------------------------"
    );

    for contention in contentions {
        for &threads in &thread_counts {
            let config = WorkloadConfig {
                num_threads: threads,
                contention,
                write_ratio: 0.3, // 70% Read / 30% Write
                keys_per_tx,
                test_duration: durations,
            };

            // 1. Run Serial Engine
            let serial_engine = SerialEngine::new();
            let serial_res = run_benchmark("Serial", &serial_engine, config);
            print_result_row(&serial_res);

            // 2. Run Parallel Engine
            let parallel_engine = ParallelEngine::new();
            let parallel_res = run_benchmark("Parallel", &parallel_engine, config);
            print_result_row(&parallel_res);

            println!(
                "---------------------------------------------------------------------------------------------------------------------"
            );
        }
    }
}

fn print_result_row(res: &BenchmarkResult) {
    println!(
        "{:<10} | {:<15} | {:<7} | {:<12} | {:<12} | {:<10.2}% | {:<15.2} | {:<10.2} | {:<10.2}",
        res.engine_name,
        res.config.contention.as_str(),
        res.config.num_threads,
        res.total_attempts,
        res.successful_commits,
        res.abort_rate(),
        res.throughput(),
        res.p50_latency_us,
        res.p99_latency_us
    );
}
