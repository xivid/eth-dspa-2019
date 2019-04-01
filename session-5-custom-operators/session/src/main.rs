extern crate timely;

use std::collections::HashMap;
use timely::dataflow::{ProbeHandle, Stream, Scope};
use timely::dataflow::operators::{Inspect, Probe, UnorderedInput, FrontierNotificator};
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
        let mut stash = HashMap::new();  // session_id -> list of (timestamp, action)
        let mut closing_time_to_session_ids: HashMap<_, Vec<char>> = HashMap::new();  // session closure time -> list of closing session id(s)
        let mut session_id_to_closing_time: HashMap<char, usize> = HashMap::new();  // session id -> session closure time

        self.unary_frontier(Pipeline, "SessionWindow", |cap, _info| {
            let mut vector = Vec::new();
            let mut notificator = FrontierNotificator::new();
            
            move |input, output| {
                while let Some((time, data)) = input.next() {
                    data.swap(&mut vector);
                    for (session_id, user_interaction) in vector.drain(..) {
                        stash.entry(session_id.clone())
                             .or_insert(Vec::new())
                             .push((time.time(), user_interaction.clone()));  // TODO: don't copy
                        // remove old closing time if it's within `epoch_timeout`
                        if let Some(closing_time) = session_id_to_closing_time.get(&session_id) {
                            if time.time() - closing_time > epoch_timeout {
                                if let Some(session_ids) = closing_time_to_session_ids.get_mut(closing_time) {
                                    session_ids.retain(|&x| x != session_id); // .remove(session_id);
                                }
                            }
                        }
                        // update closing time
                        let notification_time = time.time() + epoch_timeout;
                        let entry = session_id_to_closing_time.entry(session_id).or_insert(notification_time);
                        *entry = notification_time;
                        closing_time_to_session_ids.entry(notification_time)
                                                   .or_insert(Vec::new())
                                                   .push(session_id);
                        notificator.notify_at(cap.delayed(&notification_time));
                        println!("session {:?} received {:?}, to be notified at {:?} (closing time {:?})", session_id, user_interaction, notification_time, session_id_to_closing_time.get(&session_id));
                    }
                };
                
                notificator.for_each(&[input.frontier()], |time, _notificator| {
                    println!("time {:?} complete! Sending out {:?}", time, closing_time_to_session_ids.get(time.time()));
                    let mut session = output.session(&time);
                    // get all session ids to be closed at this timestamp
                    if let Some(session_ids) = closing_time_to_session_ids.remove(time.time()) {
                        for session_id in session_ids.into_iter() {
                            // get actions of this session
                            if let Some(actions) = stash.get_mut(&session_id) {                                
                                // let list = actions.drain_filter(|&(timestamp, action)| time.time() - timestamp >= epoch_timeout).map(|(timestamp, action)| action).collect::<Vec<_>>();
                                let mut i = 0;
                                let mut list = Vec::new();
                                let check = |&(timestamp, _)| time.time() - timestamp >= epoch_timeout;
                                while i != actions.len() {
                                    let pair = actions[i];
                                    if check(&mut actions[i]) {
                                        let val = actions.remove(i);
                                        list.push(val.1);
                                    } else {
                                        i += 1;
                                    }
                                }
                                println!("Giving {:?}: {:?}", session_id, list);
                                session.give((session_id, list));
                            }
                        }
                    }
                });
            }
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
