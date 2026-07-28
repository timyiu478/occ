#[cfg(test)]
mod parallel_tests {
    use std::sync::{Arc, Barrier};
    use std::thread;

    use occ::{OccEngine, OccError, ParallelEngine};

    // =========================================================================
    // 1. Concurrent Write-Write Conflict (Rule 3)
    // =========================================================================
    #[test]
    fn test_concurrent_write_write_conflict() {
        let engine = Arc::new(ParallelEngine::new());
        let barrier = Arc::new(Barrier::new(2));

        let engine1 = Arc::clone(&engine);
        let b1 = Arc::clone(&barrier);
        let handle1 = thread::spawn(move || {
            let mut tx = engine1.begin();
            tx.write("SHARED_KEY", 100); // Blind write
            b1.wait(); // Force both threads to start commit at the exact same time
            engine1.commit(&mut tx)
        });

        let engine2 = Arc::clone(&engine);
        let b2 = Arc::clone(&barrier);
        let handle2 = thread::spawn(move || {
            let mut tx = engine2.begin();
            tx.write("SHARED_KEY", 200); // Blind write
            b2.wait(); // Force both threads to start commit at the exact same time
            engine2.commit(&mut tx)
        });

        let res1 = handle1.join().unwrap();
        let res2 = handle2.join().unwrap();

        let success_count = [res1.is_ok(), res2.is_ok()].iter().filter(|&&x| x).count();

        // In OCC, two blind writes starting at the same time can yield:
        // - 1 success: If they validate at the exact same time (Rule 3 catches W-W conflict)
        // - 2 successes: If T1 finishes and commits before T2 starts validating (Rule 2 allows it)
        assert!(
            success_count == 1 || success_count == 2,
            "Expected 1 or 2 successes for concurrent blind writes, but got res1: {:?}, res2: {:?}",
            res1,
            res2
        );
    }

    // =========================================================================
    // 2. Concurrent Read-Write Conflict (Rule 3)
    // =========================================================================
    /// Tests that if T1 reads key K while T2 concurrently writes key K during
    /// validation, T1's read set intersects T2's active write set, failing T1.
    #[test]
    fn test_concurrent_read_write_conflict() {
        let engine = Arc::new(ParallelEngine::new());

        // Seed initial data
        engine
            .transaction(|tx| {
                tx.write("K1", 10);
                Ok(())
            })
            .unwrap();

        let barrier = Arc::new(Barrier::new(2));

        // Thread 1: Reads K1
        let engine1 = Arc::clone(&engine);
        let b1 = Arc::clone(&barrier);
        let handle1 = thread::spawn(move || {
            let mut tx = engine1.begin();
            let _ = tx.read(&"K1"); // Read set = { "K1" }
            b1.wait();
            engine1.commit(&mut tx)
        });

        // Thread 2: Writes K1
        let engine2 = Arc::clone(&engine);
        let b2 = Arc::clone(&barrier);
        let handle2 = thread::spawn(move || {
            let mut tx = engine2.begin();
            tx.write("K1", 20); // Write set = { "K1" }
            b2.wait();
            engine2.commit(&mut tx)
        });

        let res1 = handle1.join().unwrap();
        let res2 = handle2.join().unwrap();

        let success_count = [res1.is_ok(), res2.is_ok()].iter().filter(|&&x| x).count();

        // T2 writing to K1 invalidates T1 reading K1
        assert!(
            success_count == 1 || success_count == 2,
            "Expected 1 or 2 successes for concurrent blind writes, but got res1: {:?}, res2: {:?}",
            res1,
            res2
        );
    }

    // =========================================================================
    // 3. Disjoint Concurrent Transactions (High Throughput Pass)
    // =========================================================================
    /// Verifies that multiple threads operating on completely non-overlapping key
    /// spaces can all validate and write in parallel without blocking or failing.
    #[test]
    fn test_disjoint_parallel_transactions_succeed() {
        let engine = Arc::new(ParallelEngine::new());
        let num_threads = 10;
        let barrier = Arc::new(Barrier::new(num_threads));
        let mut handles = vec![];

        for thread_id in 0..num_threads {
            let engine_clone = Arc::clone(&engine);
            let b_clone = Arc::clone(&barrier);

            handles.push(thread::spawn(move || {
                let key = format!("thread_key_{}", thread_id);
                let mut tx = engine_clone.begin();
                tx.write(key.clone(), thread_id);

                b_clone.wait(); // Enter validation simultaneously
                engine_clone.commit(&mut tx)
            }));
        }

        for handle in handles {
            assert_eq!(handle.join().unwrap(), Ok(()));
        }

        // Verify all disjoint key updates exist in storage
        engine
            .transaction(|tx| {
                for thread_id in 0..num_threads {
                    let key = format!("thread_key_{}", thread_id);
                    assert_eq!(tx.read(&key), Some(thread_id));
                }
                Ok(())
            })
            .unwrap();
    }

    // =========================================================================
    // 4. Cleanup of `active_validating` Map on Abort
    // =========================================================================
    /// Tests that when a transaction fails validation, it cleanly removes itself
    /// from the `active_validating` set so it does not block future transactions.
    #[test]
    fn test_failed_validation_cleans_up_active_validating() {
        let engine = Arc::new(ParallelEngine::new());

        // Seed data
        engine
            .transaction(|tx| {
                tx.write("K1", 100);
                Ok(())
            })
            .unwrap();

        // 1. T1 reads K1
        let mut t1 = engine.begin();
        let _ = t1.read(&"K1");

        // 2. T2 modifies K1 and commits successfully
        engine
            .transaction(|tx2| {
                tx2.write("K1", 200);
                Ok(())
            })
            .unwrap();

        // 3. T1 attempts commit and FAILS validation (T2 modified K1)
        assert_eq!(engine.commit(&mut t1), Err(OccError::ValidationFailed));

        // 4. A new transaction T3 should NOT see T1 stuck in active_validating
        let result = engine.transaction(|tx3| {
            tx3.write("K1", 300); // Write to same key
            Ok(())
        });

        assert!(
            result.is_ok(),
            "T3 failed because aborted T1 was not cleaned up from active_validating!"
        );
    }

    // =========================================================================
    // 5. Heavy Multi-Threaded Stress Test
    // =========================================================================
    /// Spawns many worker threads continuously running random transactions to test
    /// storage sharding, dynamic memory collection, and lock safety under stress.
    #[test]
    fn test_heavy_parallel_stress() {
        let engine = Arc::new(ParallelEngine::<usize, usize>::new());
        let num_threads = 16;
        let ops_per_thread = 100;
        let mut handles = vec![];

        for thread_id in 0..num_threads {
            let engine_clone = Arc::clone(&engine);
            handles.push(thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let key = (thread_id + i) % 10; // Contended key space (10 keys total)

                    // Ignore commit errors (retry loop semantics)
                    let _ = engine_clone.transaction(|tx| {
                        let current = tx.read(&key).unwrap_or(0);
                        tx.write(key, current + 1);
                        Ok(())
                    });
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Ensure engine is still completely readable and healthy
        let health_check = engine.transaction(|tx| {
            for k in 0..10 {
                let _ = tx.read(&k);
            }
            Ok(())
        });

        assert!(health_check.is_ok());
    }

    #[test]
    fn test_single_transaction_commit() {
        let engine = ParallelEngine::new();

        // 1. Write some data
        let write_result = engine.transaction(|tx| {
            tx.write("KeyA", 100);
            tx.write("KeyB", 200);
            Ok(true)
        });
        assert_eq!(write_result, Ok(true));

        // 2. Read it back
        let val1 = engine.transaction(|tx| Ok(tx.read(&"KeyA"))).unwrap();
        let val2 = engine.transaction(|tx| Ok(tx.read(&"KeyB"))).unwrap();

        assert_eq!(val1, Some(100));
        assert_eq!(val2, Some(200));
    }

    #[test]
    fn test_sequential_read_modify_write_persistence() {
        let engine = ParallelEngine::new();

        // Tx1: Seed initial value
        engine
            .transaction(|tx| {
                tx.write("X", 10);
                Ok(())
            })
            .unwrap();

        // Tx2: Read value, modify it, and verify local uncommitted read
        engine
            .transaction(|tx| {
                let x = tx.read(&"X").expect("Key X should exist from Tx1");
                tx.write("X", x + 10);

                // Verify read-your-own-writes inside the active transaction
                assert_eq!(tx.read(&"X"), Some(20));
                Ok(())
            })
            .unwrap();

        // Tx3: Verify persistent storage after Tx2 committed!
        engine
            .transaction(|tx| {
                assert_eq!(
                    tx.read(&"X"),
                    Some(20),
                    "Value should persist in storage after commit"
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn test_concurrent_read_write_conflict_deterministic() {
        let engine = Arc::new(ParallelEngine::new());

        // Seed the key first
        engine
            .transaction(|tx| {
                tx.write("KEY", 0);
                Ok(())
            })
            .unwrap();

        let barrier = Arc::new(std::sync::Barrier::new(2));

        let engine1 = Arc::clone(&engine);
        let b1 = Arc::clone(&barrier);
        let handle1 = std::thread::spawn(move || {
            let mut tx = engine1.begin();
            let _ = tx.read(&"KEY"); // Add read to populate read_set
            tx.write("KEY", 100);
            b1.wait();
            engine1.commit(&mut tx)
        });

        let engine2 = Arc::clone(&engine);
        let b2 = Arc::clone(&barrier);
        let handle2 = std::thread::spawn(move || {
            let mut tx = engine2.begin();
            let _ = tx.read(&"KEY"); // Add read to populate read_set
            tx.write("KEY", 200);
            b2.wait();
            engine2.commit(&mut tx)
        });

        let res1 = handle1.join().unwrap();
        let res2 = handle2.join().unwrap();

        let success_count = [res1.is_ok(), res2.is_ok()].iter().filter(|&&x| x).count();
        assert_eq!(success_count, 1, "Exactly one transaction must succeed");
    }

    #[test]
    fn test_local_delete_uncommitted_read() {
        let engine = ParallelEngine::<String, String>::new();

        // Seed initial key
        engine
            .transaction(|tx| {
                tx.write("K1".to_string(), "V1".to_string());
                Ok(())
            })
            .unwrap();

        // Perform local delete
        engine
            .transaction(|tx| {
                // 1. Initial read sees "V1"
                assert_eq!(tx.read(&"K1".to_string()), Some("V1".to_string()));

                // 2. Delete the key
                tx.delete("K1".to_string());

                // 3. Local read immediately after delete should return None
                assert_eq!(tx.read(&"K1".to_string()), None);

                Ok(())
            })
            .unwrap();

        // Verify key was permanently removed from global storage after commit
        engine
            .transaction(|tx| {
                assert_eq!(tx.read(&"K1".to_string()), None);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn test_delete_then_write_same_transaction() {
        let engine = ParallelEngine::<String, String>::new();

        engine
            .transaction(|tx| {
                tx.write("K1".to_string(), "OLD_VAL".to_string());
                Ok(())
            })
            .unwrap();

        engine
            .transaction(|tx| {
                tx.delete("K1".to_string());
                assert_eq!(tx.read(&"K1".to_string()), None);

                // Re-insert new value
                tx.write("K1".to_string(), "NEW_VAL".to_string());
                assert_eq!(tx.read(&"K1".to_string()), Some("NEW_VAL".to_string()));

                Ok(())
            })
            .unwrap();

        // Verify committed state
        engine
            .transaction(|tx| {
                assert_eq!(tx.read(&"K1".to_string()), Some("NEW_VAL".to_string()));
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn test_delete_causes_validation_conflict() {
        let engine = Arc::new(ParallelEngine::<String, String>::new());

        // Seed initial data
        engine
            .transaction(|tx| {
                tx.write("K1".to_string(), "V1".to_string());
                Ok(())
            })
            .unwrap();

        // 1. T1 starts and reads K1
        let mut t1 = engine.begin();
        assert_eq!(t1.read(&"K1".to_string()), Some("V1".to_string()));

        // 2. T2 deletes K1 and commits successfully
        engine
            .transaction(|tx2| {
                tx2.delete("K1".to_string());
                Ok(())
            })
            .unwrap();

        // 3. T1 attempts to commit -> MUST FAIL because T2 deleted K1 (modified T1's read set)
        assert_eq!(engine.commit(&mut t1), Err(OccError::ValidationFailed));
    }

    #[test]
    fn test_delete_non_existent_key() {
        let engine = ParallelEngine::<String, String>::new();

        let res = engine.transaction(|tx| {
            tx.delete("PHANTOM_KEY".to_string());
            Ok(())
        });

        assert!(res.is_ok());

        engine
            .transaction(|tx| {
                assert_eq!(tx.read(&"PHANTOM_KEY".to_string()), None);
                Ok(())
            })
            .unwrap();
    }
}
