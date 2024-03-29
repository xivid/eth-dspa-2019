extern crate timely;

use std::io::{BufReader, BufRead};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Input, Map, Inspect, Probe};
use timely::dataflow::operators::aggregation::Aggregate;

fn hash_str<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn main() {
    // 1) Instantiate a computation pipeline by chaining operators
    timely::execute_from_args(std::env::args(), |worker| {

        let index = worker.index();
        // create opaque handles to feed input and monitor progress
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        worker.dataflow(|scope| {
           scope.input_from(&mut input)
                .flat_map(|text: String| 
                   text.split_whitespace()
                       .map(move |word| (word.to_owned(), 1))
                       .collect::<Vec<_>>()
                )
                .aggregate(
                   // fold: combines new data with existing state
                   |_key, val, agg| { *agg += val; },
                   // emit: produce output from state
                   |key, agg: i64| (key, agg),
                   // hash: route data according to a key
                   |key| hash_str(key)
                )
                .inspect(move |(word, count)| println!("worker: #{}\tword: {}\tcount: {}", index, word, count))
                .probe_with(&mut probe);
        });

        let path = format!("/home/zhifei/repo/dspa/session-2-timely/input/input-{}.txt", index);
        let file = File::open(path).expect("Input data not found in CWD");
        let buffered = BufReader::new(file);
        // send input line-by-line
        let mut total_lines = 0;
        for line in buffered.lines() {
            input.send(line.unwrap());
            total_lines = total_lines + 1;
        }
        println!("worker #{} sent {} lines", index, total_lines);
        // advance input and process
        input.advance_to(total_lines);  // total_lines or total_lines + 1?
        println!("worker #{} advanced to {}, time now {}", index, total_lines, input.time());
        let mut step = 0;
        while probe.less_than(input.time()) {
            println!("worker #{} takes step {}, input.time() is {}...", index, step, input.time());
            step += 1;
            worker.step();
        }
    }).unwrap();
}