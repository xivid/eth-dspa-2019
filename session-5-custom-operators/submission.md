# Assignment 5

## Task 1

In the dataflow model, in contrast to batch processing, data is processed in a **pipeline** of operators. If states are arbitrarily shared across the pipeline, a possible problem is that as soon as an upstream operator updates a shared state, a downstream operator will observe the new value immediately, even if the upstream operator is not meant to let the downstream operator consume the new value. 

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
        input.unary(Pipeline, "Average", move |_, _| {
                let mut vector = Vec::new();
                move |input, output| {
                    input.for_each(|time, data| {
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

Formulating it as a reusable operator:

```rust
trait Average<G: Scope> {
    fn average(&self) -> Stream<G, (u64, f64)>;
}
impl<G: Scope> Average<G> for Stream<G, u64> {
    fn average(&self) -> Stream<G, (u64, f64)> {
    // ... (to be filled in) …
    }
}
```

and changes that need to be made within my code is:

```rust
// TODO
```

Further, using generics:

```rust
extern crate Num;
```

## Task 3

To emit the average once per epoch while taking best advantage of worker parallelism, we can ...


## Task 4

`reorder` operator


## Task 5

session window