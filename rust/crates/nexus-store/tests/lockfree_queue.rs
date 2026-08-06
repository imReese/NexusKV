use nexus_store::lockfree_queue::LockFreeBlockQueue;
use std::thread;

#[test]
fn test_lockfree_queue_single_thread() {
    let queue = LockFreeBlockQueue::new(8);
    assert_eq!(queue.capacity(), 8);

    assert!(queue.try_push(101).is_ok());
    assert!(queue.try_push(102).is_ok());

    let val1 = queue.try_pop();
    assert_eq!(val1, Some(101));

    let val2 = queue.try_pop();
    assert_eq!(val2, Some(102));
}

#[test]
fn test_lockfree_queue_multithreaded_mpmc() {
    let queue = LockFreeBlockQueue::new(100);

    let q1 = queue.clone();
    let producer1 = thread::spawn(move || {
        for i in 0..20 {
            let _ = q1.try_push(i);
        }
    });

    let q2 = queue.clone();
    let producer2 = thread::spawn(move || {
        for i in 20..40 {
            let _ = q2.try_push(i);
        }
    });

    producer1.join().unwrap();
    producer2.join().unwrap();

    let mut popped_count = 0;
    for _ in 0..40 {
        if queue.try_pop().is_some() {
            popped_count += 1;
        }
    }
    assert_eq!(popped_count, 40);
}
