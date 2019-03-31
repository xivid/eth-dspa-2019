extern crate timely;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Inspect, ToStream};
use timely::dataflow::operators::generic::operator::Operator;
// use timely::Data;
use timely::dataflow::{Stream, Scope};

trait Average<G: Scope> {
    fn average(&self) -> Stream<G, (u64, f64)>;
}
impl<G: Scope> Average<G> for Stream<G, u64> {
    fn average(&self) -> Stream<G, (u64, f64)> {
       let mut vector = Vec::new();
       let mut sum = 0.0;
       let mut count = 0.0;
       self.unary(Pipeline, "Average", move |_, _| move |input, output| {
           input.for_each(|time, data| {
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

fn main() {
    timely::example(|scope| {

        let input = (0..10).to_stream(scope);
        input.average()
             .inspect(|x| println!("seen: {:?}", x));
    });
}
