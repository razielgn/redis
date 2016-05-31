#![feature(plugin)]
#![plugin(quickcheck_macros)]

#[macro_use]
extern crate nom;

#[cfg(test)]
extern crate quickcheck;

mod redis;

fn main() {
    redis::repl::repl();
}
