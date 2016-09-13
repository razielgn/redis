#![feature(advanced_slice_patterns, slice_patterns)]

#[macro_use]
extern crate nom;

#[cfg(test)]
#[macro_use]
extern crate quickcheck;

extern crate mioco;

mod redis;

fn main() {
    redis::tcp::listen_async();
}
