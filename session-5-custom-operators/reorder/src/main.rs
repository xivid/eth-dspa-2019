extern crate timely;

use std::collections::HashMap;
use timely::dataflow::operators::{Operator, UnorderedInput, Inspect};
use timely::dataflow::channels::pact::Pipeline;

fn main() {
    // 1) Instantiate a computation pipeline by chaining operators
    timely::execute_from_args(std::env::args(), |worker| {
        // let index = worker.index();

        let (mut input, mut cap) = worker.dataflow::<usize, _, _>(|scope| {
            let (input, stream) = scope.new_unordered_input();
            let mut stash = HashMap::new();

            stream.unary_notify(Pipeline, "Reorder", vec![], move |input, output, barrier| {
                    while let Some((time, data)) = input.next() {
                        stash.entry(time.time().clone())
                             .or_insert(Vec::new())
                             .push(data.replace(Vec::new()));
                        barrier.notify_at(time.retain());
                        // output.session(&time).give(data);
                    }
                    // ... except print out when each time completes.
                    while let Some((time, count)) = barrier.next() {
                        println!("time {:?} complete with count {:?}!", time, count);
                        let mut session = output.session(&time);
                        if let Some(list) = stash.remove(time.time()) {
                            for mut vector in list.into_iter() {
                                session.give_vec(&mut vector);
                            }
                        }
                    }
                }).inspect_batch(move |epoch, data| {
                    for d in data {
                        println!("@t={} | seen: {:?}", epoch, d);
                    }
                });
            input // returns a pair (input::UnorderedHandle, Capability)
        });
        
        // Generate out-of-order inputs
        input.session(cap.delayed(&2)).give('B');
        input.session(cap.delayed(&1)).give('A');
        input.session(cap.delayed(&2)).give('b');
        input.session(cap.delayed(&3)).give('C');
        input.session(cap.delayed(&3)).give('c');
        input.session(cap.delayed(&1)).give('a');

    }).unwrap();
}