#[cfg(test)]
mod serial_tests {
    use std::sync::Arc;
    use std::thread;
    use occ::{OccEngine, SerialEngine, OccError};

    #[test]
    fn test_single_transaction_commit() {
        let engine = SerialEngine::new();

        // 1. Write some data
        let write_result = engine.transaction(|tx| {
            tx.write("KeyA", 100);
            tx.write("KeyB", 200);
            Ok(true)
        });
        assert_eq!(write_result, Ok(true));

        // 2. Read it back
        let val1 = engine.transaction(|tx| {
            Ok(tx.read(&"KeyA"))
        }).unwrap();
        let val2 = engine.transaction(|tx| {
            Ok(tx.read(&"KeyB"))
        }).unwrap();
        
        assert_eq!(val1, Some(100));
        assert_eq!(val2, Some(200));
    }

    #[test]
    fn test_read_your_own_writes() {
        let engine = SerialEngine::new();
        
        engine.transaction(|tx| {
            // Write and immediately read within the same transaction
            tx.write("Secret", 42);
            let val = tx.read(&"Secret");
            
            assert_eq!(val, Some(42));
            
            // Delete and immediately verify it's gone locally
            tx.delete("Secret");
            assert_eq!(tx.read(&"Secret"), None);
            
            Ok(())
        }).unwrap();
    }

    #[test]
    fn test_sequential_read_modify_write_persistence() {
        let engine = SerialEngine::new();
        
        // Tx1: Seed initial value
        engine.transaction(|tx| {
            tx.write("X", 10);
            Ok(())
        }).unwrap();

        // Tx2: Read value, modify it, and verify local uncommitted read
        engine.transaction(|tx| {
            let x = tx.read(&"X").expect("Key X should exist from Tx1");
            tx.write("X", x + 10);

            // Verify read-your-own-writes inside the active transaction
            assert_eq!(tx.read(&"X"), Some(20));
            Ok(())
        }).unwrap();

        // Tx3: Verify persistent storage after Tx2 committed!
        engine.transaction(|tx| {
            assert_eq!(tx.read(&"X"), Some(20), "Value should persist in storage after commit");
            Ok(())
        }).unwrap();
    }


    #[test]
    fn test_concurrent_non_conflicting_transactions() {
        // Use Arc to share the engine across threads
        let engine = Arc::new(SerialEngine::new());
        let e1 = Arc::clone(&engine);
        let e2 = Arc::clone(&engine);

        // Thread 1 writes to X
        let t1 = thread::spawn(move || {
            e1.transaction(|tx| {
                tx.write("X", 10);
                Ok(())
            })
        });

        // Thread 2 writes to Y
        let t2 = thread::spawn(move || {
            e2.transaction(|tx| {
                tx.write("Y", 20);
                Ok(())
            })
        });

        // Both should succeed because their write sets don't overlap 
        // with each other's read sets
        assert!(t1.join().unwrap().is_ok());
        assert!(t2.join().unwrap().is_ok());

        // Verify final state
        engine.transaction(|tx| {
            assert_eq!(tx.read(&"X"), Some(10));
            assert_eq!(tx.read(&"Y"), Some(20));
            Ok(())
        }).unwrap();
    }

    #[test]
    fn test_concurrent_conflicting_transactions() {
        let engine = SerialEngine::new();
        
        // Initial setup
        engine.transaction(|tx| {
            tx.write("Counter", 10);
            Ok(())
        }).unwrap();

        // T1: Starts and reads the counter
        let mut tx1 = engine.begin();
        let val1 = tx1.read(&"Counter").unwrap(); // Reads 10
        
        // T2: Starts, reads, writes, and commits BEFORE T1 finishes
        engine.transaction(|tx2| {
            let val2 = tx2.read(&"Counter").unwrap();
            tx2.write("Counter", val2 + 5);
            Ok(())
        }).unwrap(); // T2 successfully commits "15"

        // T1: Now tries to write and commit its stale view of the world
        tx1.write("Counter", val1 + 5);
        let result = engine.commit(&mut tx1);

        // T1 MUST fail validation because T2 modified "Counter", which T1 read.
        assert_eq!(result, Err(OccError::ValidationFailed));

        // Verify the final value is safely 15, not 20!
        engine.transaction(|tx| {
            assert_eq!(tx.read(&"Counter"), Some(15));
            Ok(())
        }).unwrap();
    }
    
    #[test]
    fn test_read_only_transactions_do_not_conflict() {
        let engine = SerialEngine::new();
        engine.transaction(|tx| { tx.write("Data", 99); Ok(()) }).unwrap();

        let mut tx1 = engine.begin();
        let mut tx2 = engine.begin();

        // Both transactions read the same data
        assert_eq!(tx1.read(&"Data"), Some(99));
        assert_eq!(tx2.read(&"Data"), Some(99));

        // Neither writes data, so both should commit successfully without conflicts
        assert_eq!(engine.commit(&mut tx1), Ok(()));
        assert_eq!(engine.commit(&mut tx2), Ok(()));
    }

    #[test]
    fn test_user_abort_rolls_back_changes() {
        let engine = SerialEngine::new();

        // 1. Seed initial data
        engine.transaction(|tx| {
            tx.write("AccountA", 100);
            Ok(())
        }).unwrap();

        // 2. Start a transaction, stage local writes, then manually return an error
        let abort_result: Result<(), OccError> = engine.transaction(|tx| {
            let balance = tx.read(&"AccountA").expect("Key should exist");
            
            // Stage a write locally
            tx.write("AccountA", balance - 50);

            // Simulate business logic failure or manual rollback
            Err(OccError::UserAbort("Insufficient funds or manual cancellation".to_string()))
        });

        // 3. Assert that engine.transaction() returned the exact UserAbort error
        assert_eq!(
            abort_result,
            Err(OccError::UserAbort("Insufficient funds or manual cancellation".to_string()))
        );

        // 4. Verify that global storage remains untouched (value is still 100, not 50)
        engine.transaction(|tx| {
            assert_eq!(
                tx.read(&"AccountA"),
                Some(100),
                "AccountA should remain unchanged after user abort"
            );
            Ok(())
        }).unwrap();
    }
    #[test]
    fn test_delete_propagation_and_conflict() {
        let engine = SerialEngine::new();

        // Seed initial data
        engine.transaction(|tx| {
            tx.write("K1", 100);
            Ok(())
        }).unwrap();

        // T1 reads K1
        let mut t1 = engine.begin();
        assert_eq!(t1.read(&"K1"), Some(100));

        // T2 deletes K1 and commits
        engine.transaction(|tx2| {
            tx2.delete("K1");
            Ok(())
        }).unwrap();

        // T1 MUST fail validation because T2 deleted K1 (which T1 read)
        assert_eq!(engine.commit(&mut t1), Err(OccError::ValidationFailed));

        // Verify K1 is completely gone from global storage
        engine.transaction(|tx| {
            assert_eq!(tx.read(&"K1"), None);
            Ok(())
        }).unwrap();
    }
}
