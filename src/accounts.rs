use crate::books::{BalanceType, BookId};
use crate::currency::Currency;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub(crate) type AccountId = u64;

#[derive(Deserialize, Serialize, Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum AccountType {
    Debit,
    Credit,
}

#[derive(Debug)]
pub(crate) struct Account {
    pub(crate) account_type: AccountType,
    pub(crate) books: HashMap<(Currency, BalanceType), BookId>,
}
