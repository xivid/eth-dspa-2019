# Assignment 5

## Task 1

In the dataflow model, in contrast to batch processing, data is processed in a **pipeline** of operators, this can cause a problem of *Stream synchronisation*. If states are arbitrarily shared across the pipeline, a possible problem is that as soon as an upstream operator updates a shared state, a downstream operator will observe the new value immediately, even if the upstream operator is not meant to let the downstream operator consume the new value. 

In our example, when the third `map` operator attempts to calculate the running average for all items until `x`, it is very likely that both `sum` and `count` have been updated by the other `map` operators which have received later items.

To enable inter-operator communication while respecting the shared-nothing principle, a possible approach is to explicitly put the data to be communicated into the output of an operator and serve as the input of another operator, so that every operator sees the values it is meant to see.

## Task 2

The operator is implemented as:

```rust
extern crate timely;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Inspect, ToStream};
use timely::dataflow::operators::generic::operator::Operator;
fn main() {
    timely::example(|scope| {
        let mut sum = 0.0;
        let mut count = 0.0;

        let input = (0..10).to_stream(scope);
        input.unary(Pipeline, "Average", move |_, _| move |input, output| {
                    input.for_each(|time, data| {
                        let mut vector = Vec::new();  // give back a new vector to Timely
                        data.swap(&mut vector);
                        let mut session = output.session(&time);
                        for datum in vector.drain(..) {
                            sum += datum as f64;
                            count += 1.0;
                            session.give((datum, sum / count))
                        }
                    });
                }
            })
            .inspect(|x| println!("seen: {:?}", x));
    });
}
```

To formulate it as a reusable operator:

```rust
trait Average<G: Scope> {
    fn average(&self) -> Stream<G, (u64, f64)>;
}
impl<G: Scope> Average<G> for Stream<G, u64> {
    fn average(&self) -> Stream<G, (u64, f64)> {
       let mut sum = 0.0;
       let mut count = 0.0;
       self.unary(Pipeline, "Average", move |_, _| move |input, output| {
           input.for_each(|time, data| {
               let mut vector = Vec::new();
               data.swap(&mut vector);
               for datum in vector.drain(..) {
                   sum += datum as f64;
                   count += 1.0;
                   output.session(&time).give((datum, sum / count));
               }
           });
       })
    }
}
```

We need to add `use timely::dataflow::{Stream, Scope};` in the head, and the main function becomes:

```rust
fn main() {
    timely::example(|scope| {
        let input = (0..10).to_stream(scope);
        input.average()
             .inspect(|x| println!("seen: {:?}", x));
    });
}
```

Further, using generics:

```rust
extern crate Num;
// TODO convert input to f64 and keep everything else
```

## Task 3

First, in each epoch, the tuples should be uniformly distributed among all workers so that the advantage of worker parallism is taken at the best, and each worker handles a part of the tuples. The partitioning strategy should be as equal among workers as possible, for example, in a round-robin manner. Secondly, the operator should maintain two worker-local aggregate states `sum` and `count` for the tuples it received within one epoch. After one epoch is finished, there should be another stage to accumulate `sum` and `count` from all workers to one "master" worker, and this worker finally outputs the global average of this epoch.


## Task 4

A HashMap is used to stash data. The data is not emitted until the notification of the completion of its epoch.

```rust
extern crate timely;

use std::collections::HashMap;
use timely::dataflow::operators::{Operator, UnorderedInput, Inspect};
use timely::dataflow::channels::pact::Pipeline;
use timely::Data;
use timely::dataflow::{Stream, Scope};

trait Reorder<G: Scope, D: Data> {
    fn reorder(&self) -> Stream<G, D>;
}
impl<G: Scope, D: Data> Reorder<G, D> for Stream<G, D> {
    fn reorder(&self) -> Stream<G, D> {
        let mut stash = HashMap::new();
        self.unary_notify(Pipeline, "Reorder", vec![], move |input, output, barrier| {
            while let Some((time, data)) = input.next() {
                stash.entry(time.time().clone())
                        .or_insert(Vec::new())
                        .push(data.replace(Vec::new()));
                barrier.notify_at(time.retain());
            }
            // when notified
            while let Some((time, count)) = barrier.next() {
                println!("time {:?} complete with count {:?}!", time, count);
                let mut session = output.session(&time);
                if let Some(list) = stash.remove(time.time()) {
                    for mut vector in list.into_iter() {
                        session.give_vec(&mut vector);
                    }
                }
            }
        })
    }
}

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let (mut input, cap) = worker.dataflow::<usize, _, _>(|scope| {
            let (input, stream) = scope.new_unordered_input();
            stream
                .reorder()
                .inspect_batch(move |epoch, data| {
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
```

The output is:
```
time Capability { time: 1, internal: ... } complete with count 1!
time Capability { time: 2, internal: ... } complete with count 1!
time Capability { time: 3, internal: ... } complete with count 1!
@t=1 | seen: 'A'
@t=1 | seen: 'a'
@t=2 | seen: 'B'
@t=2 | seen: 'b'
@t=3 | seen: 'C'
@t=3 | seen: 'c'
```

## Task 5

session window
        // Exchange::new(|(session_id, _)| *session_id)
