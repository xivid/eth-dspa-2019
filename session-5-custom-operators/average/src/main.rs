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
                let mut vector = Vec::new();
                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    let mut session = output.session(&time);
                    for datum in vector.drain(..) {
                        sum += datum as f64;
                        count += 1.0;
                        session.give((datum, sum / count))
                    }
                });
            })
            .inspect(|x| println!("seen: {:?}", x));
    });
}
