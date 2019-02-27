use std::io::{BufReader, BufRead};
use std::fs::File;

fn main() {
    println!("Hello, world!");

    let file = File::open("/home/zhifei/repo/dspa/session-2-timely/input-0.txt").expect("Input data not found in CWD");
    let mut buffered = BufReader::new(file);

    let mut total_lines = 0;
    let mut line = String::new();
    while buffered.read_line(&mut line).expect("reading from cursor won't fail") > 0 {
        println!("Read: [{}]", line);
        for s in line.split_whitespace() {
            println!("  Word: [{}]", s);
        }
        total_lines += 1;
        if total_lines % 5 == 0 {
            line.clear();
        }
    }
}
