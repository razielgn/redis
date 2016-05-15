#[macro_use]
extern crate nom;

mod redis;

fn main() {
    redis::repl::repl();
}
