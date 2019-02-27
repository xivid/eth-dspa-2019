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

        let path = format!("/home/zhifei/repo/dspa/session-2-timely/input-{}.txt", index);
        let file = File::open(path).expect("Input data not found in CWD");
        let mut buffered = BufReader::new(file);

        let mut total_lines = 0;
        let mut lines = String::new();
        let mut round = 0;
        while buffered.read_line(&mut lines).expect("Unable to read a line") > 0 {
            total_lines += 1;
            if total_lines % 5 == 0 {
                println!("Worker #{} Input Lines {}~{}:", index, total_lines - 4, total_lines);
                // send buffered 5 lines to input, create a new empty buffer
                input.send(lines);  // what about cloning here and clearing next?
                lines = String::new();
                // advance input and process
                input.advance_to(round + 1);
                // println!("worker #{} advanced to {}, time now {}", index, round + 1, input.time());
                while probe.less_than(input.time()) {
                    worker.step();
                }

                // next round
                round += 1;
            }
        }
        // the last few lines
        if total_lines % 5 != 0 {
            println!("Worker #{} Final Input Lines {}~{}:", index, total_lines - total_lines % 5 + 1, total_lines);
            input.send(lines);
            
            // advance input and process
            input.advance_to(round + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
    }).unwrap();
}