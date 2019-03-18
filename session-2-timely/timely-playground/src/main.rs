extern crate timely;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Input, Exchange, Inspect, Probe};

fn main() {
    // 1) Instantiate a computation pipeline by chaining operators
    timely::execute_from_args(std::env::args(), |worker| {

        let index = worker.index();
        // create opaque handles to feed input and monitor progress
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        worker.dataflow(|scope| {
            scope.input_from(&mut input)
            .exchange(|(_round, num)| *num)
            .inspect(move |(round, num)| println!("round: #{}\tnum: {}\tworker: {}", round, num, index))
            .probe_with(&mut probe);
        });

        let mut step = 0;
        // 2) Push data into the dataflow and allow computation to run
        for round in 0..10 {
            for j in 0..round + 1 {
                if index == 0 {
                    input.send((round, j));
                    println!("worker #{} sent ({}, {})", index, round, j);
                }
                // advance input and instruct the workers to do work
                input.advance_to(round*(round + 1)/2 + j + 1);
                println!("worker #{} advanced to {}, time now {}", index, round*(round + 1)/2 + j + 1, input.time());

                while probe.less_than(input.time()) {
                    println!("worker #{} takes step {}, input.time() is {}...", index, step, input.time());
                    step += 1;
                    worker.step();
                }
            }
        }
    }).unwrap();
}