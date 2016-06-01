#![cfg_attr(test, feature(plugin))]
#![cfg_attr(test, plugin(quickcheck_macros))]

#[macro_use]
extern crate nom;

#[cfg(test)]
extern crate quickcheck;

mod redis;

fn main() {
    redis::repl::repl();
}
