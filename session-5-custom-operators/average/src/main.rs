extern crate num_traits;
extern crate timely;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Inspect, ToStream};
use timely::dataflow::operators::generic::operator::Operator;
use timely::Data;
use num_traits::Num;
use num_traits::cast::ToPrimitive;
use timely::dataflow::{Stream, Scope};

trait NumData: Num + ToPrimitive + Copy + Data {}
impl<T: Num + ToPrimitive + Copy + Data> NumData for T {}

trait Average<G: Scope, D: NumData> {
    fn average(&self) -> Stream<G, (D, f64)>;
}
impl<G: Scope, D: NumData> Average<G, D> for Stream<G, D> {
    fn average(&self) -> Stream<G, (D, f64)> {
       let mut sum = 0.0;
       let mut count = 0.0;
       self.unary(Pipeline, "Average", move |_, _| move |input, output| {
           input.for_each(|time, data| {
               let mut vector = Vec::new();
               data.swap(&mut vector);
               for datum in vector.drain(..) {
                   sum += datum.to_u64().unwrap() as f64;
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
