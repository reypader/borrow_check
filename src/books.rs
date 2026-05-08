use serde::{Deserialize, Serialize};

pub(crate) type BookId = u64;

#[derive(Deserialize, Serialize, Eq, Hash, PartialEq, Copy, Clone, Debug)]
pub(crate) enum BalanceType {
    Current,
    Available,
    Hold,
}
