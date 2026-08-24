#![feature(test)]

extern crate test;

#[path = "../src/thread_pool.rs"]
mod thread_pool;

use {
    std::{
        hint::spin_loop,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::Duration,
    },
    test::Bencher,
    thread_pool::{WorkerJob, WorkerPool},
};

const NUM_WORKERS: usize = 48;
const JOB_QUEUE_CAPACITY: usize = 4_0960;
const SATURATED_BATCH_SIZE: usize = JOB_QUEUE_CAPACITY;
const WORKER_IDLE_DURATION: Duration = Duration::from_micros(100);

#[repr(align(64))]
struct Completion(AtomicBool);

struct CompletionJob(Arc<Completion>);

impl WorkerJob for CompletionJob {
    fn run(self) {
        self.0.0.store(true, Ordering::Release);
    }
}

fn bench_worker_pool(
    bencher: &mut Bencher,
    jobs_per_batch: usize,
    idle_duration: Option<Duration>,
) {
    let worker_pool = WorkerPool::new("solBenchPool", NUM_WORKERS, JOB_QUEUE_CAPACITY);
    assert_eq!(worker_pool.num_workers(), NUM_WORKERS);
    let completions = (0..jobs_per_batch)
        .map(|_| Arc::new(Completion(AtomicBool::new(false))))
        .collect::<Vec<_>>();

    bencher.iter(|| {
        for completion in &completions {
            completion.0.store(false, Ordering::Relaxed);
        }
        if let Some(idle_duration) = idle_duration {
            thread::sleep(idle_duration);
        }

        for completion in &completions {
            worker_pool.send(CompletionJob(Arc::clone(completion)));
        }
        for completion in &completions {
            while !completion.0.load(Ordering::Acquire) {
                spin_loop();
            }
        }
    });
}

#[bench]
fn wake_one_48(bencher: &mut Bencher) {
    bench_worker_pool(bencher, 1, Some(WORKER_IDLE_DURATION));
}

#[bench]
fn wake_burst_48(bencher: &mut Bencher) {
    bench_worker_pool(bencher, NUM_WORKERS, Some(WORKER_IDLE_DURATION));
}

#[bench]
fn saturated_48(bencher: &mut Bencher) {
    bench_worker_pool(bencher, SATURATED_BATCH_SIZE, None);
}
