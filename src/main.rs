#![cfg_attr(test, feature(plugin))]
#![cfg_attr(test, plugin(quickcheck_macros))]

#![feature(advanced_slice_patterns, slice_patterns)]

#[macro_use]
extern crate nom;

#[cfg(test)]
extern crate quickcheck;

extern crate mioco;

mod redis;

fn main() {
    redis::tcp::listen_async();
}
