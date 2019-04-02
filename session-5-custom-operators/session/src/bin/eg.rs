extern crate timely;

use std::collections::HashMap;
use timely::dataflow::{ProbeHandle, Stream, Scope};
use timely::dataflow::operators::{Inspect, Probe, UnorderedInput};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::channels::pact::Pipeline;

#[derive(Clone, Debug)]
enum Action {
    Start,
    PageLoad,
    Click,
    KeyPress(char),
}

trait SessionWindow<G: Scope> {
    fn sessionize(&self, epoch_timeout: usize) -> Stream<G, (char, Vec<Action>)>;
}

impl<G: Scope<Timestamp=usize>> SessionWindow<G> for Stream<G, (char, Action)> {
    fn sessionize(&self, epoch_timeout: usize) -> Stream<G, (char, Vec<Action>)> {
        let mut buffer = Vec::new();
        let mut data_stash = HashMap::new();
        let mut session_expiry = HashMap::new();  // Time -> Vec<SessionId>
        let mut session_window = HashMap::new();  // SessionId -> (Time, Vec<Action>)
        self.unary_notify(Pipeline, "Sessionize", vec![], move |input, output, notificator| {
            input.for_each(|time, data| {
                // Delay alterations to operator state until each timestamp is complete
                data.swap(&mut buffer);
                data_stash.entry(*time.time())
                    .or_insert_with(Vec::new)
                    .extend(buffer.drain(..));
                notificator.notify_at(time.retain());
            });
            notificator.for_each(|time, _cnt, notify| {
                if let Some(buffer) = data_stash.remove(time.time()) {
                    for elem in buffer {
                        // Re-organize all data by originating session (opens window or merges)
                        let (ref mut last_active, ref mut actions) = session_window.entry(elem.0).or_insert((0, vec![]));
                        assert!(*last_active <= *time.time());
                        *last_active = *time.time();
                        actions.push(elem.1);
                        // Set alarm assuming session will be inactive (triggers loop below)
                        session_expiry.entry(*time.time() + epoch_timeout)
                            .or_insert_with(Vec::new)
                            .push(elem.0);
                        notify.notify_at(time.delayed(&(time.time() + epoch_timeout)));
                    }
                }
                if let Some(session_ids) = session_expiry.remove(time.time()) {
                    for sid in session_ids {
                        // Double-check that no new intervening actions occurred in this session
                        if let Some((ref last_active, ref mut actions)) = session_window.get_mut(&sid) {
                            if *time.time() >= last_active + epoch_timeout {
                                // Close the session window and flush all internal state
                                output.session(&time).give((sid, actions.drain(..).collect()));
                            }
                        }
                    }
                }
            });
        })
    }
}

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        // Schema: (epoch, (session_id: char, user_interaction: Action))
        let input_records = vec![
            // Case 1: burst of activity with no inactivity gap
            (0, ('A', Action::Start)),
            (10, ('A', Action::PageLoad)),
            (50, ('A', Action::Click)),
            (100, ('A', Action::KeyPress('h'))),
            (150, ('A', Action::KeyPress('o'))),
            (200, ('A', Action::Click)),
            (250, ('A', Action::KeyPress('w'))),
            (250, ('A', Action::KeyPress('d'))),
            (300, ('A', Action::KeyPress('y'))),
            (350, ('A', Action::Click)),

            // Case 2: user took a break so their session is fragmented into several pieces
            (0, ('B', Action::Start)),
            (20, ('B', Action::PageLoad)),
            (25, ('B', Action::KeyPress('f'))),
            (26, ('B', Action::KeyPress('u'))),
            (27, ('B', Action::KeyPress('n'))),

            (1000, ('B', Action::Click)),
            (1050, ('B', Action::Click)),
            (1100, ('B', Action::KeyPress(':'))),
            (1110, ('B', Action::KeyPress('-'))),
            (1120, ('B', Action::KeyPress(')'))),

            (2000, ('B', Action::Click)),
            (2001, ('B', Action::Click)),
            (2002, ('B', Action::Click)),
            (2002, ('B', Action::Click)),
            (2003, ('B', Action::Click)),
        ];

        // Construct the dataflow graph
        let mut probe = ProbeHandle::new();
        let (mut input, cap0) = worker.dataflow::<usize, _, _>(|scope| {
            let (input, stream) = scope.new_unordered_input();
            stream
                .sessionize(500)
                .inspect_batch(move |epoch, data| {
                    for d in data {
                        println!("@t={} | seen: {:?}", epoch, d);
                    }
                })
                .probe_with(&mut probe);
            input
        });

        // Feed the inputs and let the computation run
        let max_time = *input_records.iter().map(|(t, _)| t).max().unwrap() + 1;
        for (time, data) in input_records {
            input.session(cap0.delayed(&time)).give(data);
        }
        drop(input);
        drop(cap0);  // (Here we promise not to produce any outputs for time 0 or later)

        while probe.less_than(&max_time) {
            worker.step();
        }
    }).unwrap();
}