use prusti_contracts::*;
use prusti_refinement::abs_types::AbsSeq;

#[trusted]
#[pure]
pub fn slice_to_seq(slice: &[u8]) -> AbsSeq<u8> {
    #![allow(unused)]
    AbsSeq::empty()
}

#[trusted]
#[ensures(forall(|a: &[u8], b: &[u8]| slice_to_seq(a) === slice_to_seq(b) ==> a === b))]
pub fn axiom_slice_to_seq_injective() {}

// Workaround for extern spec of Clone trait impl not working
#[trusted]
#[ensures(result === s)]
pub fn clone_slice(s: &Box<[u8]>) -> Box<[u8]> {
    s.clone()
}
