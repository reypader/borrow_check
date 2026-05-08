use crate::accounts::{Account, AccountId, AccountType};
use crate::books::{BalanceType, BookId};
use crate::currency::Currency;
use serde::Deserialize;
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

#[derive(Deserialize, Debug)]
pub(crate) struct Operation {
    idempotency_key: Uuid,
    entries: Vec<OperationEntry>,
}

#[derive(Deserialize, Debug)]
struct OperationEntry {
    account: AccountId,
    balance_type: BalanceType,
    op_type: AccountType,
    amount: i128,
    currency: Currency,
    ledger_code: String,
}

#[derive(Debug)]
pub(crate) struct ValidOperation {
    pub(crate) idempotency_key: Uuid,
    accounts_in_scope: HashMap<AccountId, Account>,
    pub(crate) entries: Vec<PreProcessedEntry>,
}

#[derive(Debug)]
pub(crate) struct PreProcessedEntry {
    pub(crate) target_account_id: AccountId,
    target_book_id: BookId,
    pub(crate) amount: i128,
    pub(crate) ledger_code: [u8; 8],
    pub(crate) currency: Currency,
    pub(crate) balance_type: BalanceType,
}

impl ValidOperation {
    pub(crate) fn parse(
        op: Operation,
        accounts_in_scope: HashMap<AccountId, Account>,
    ) -> Result<Self, InvalidOperationError> {
        //TODO make 100 configurable
        if op.entries.len() > 100 {
            return Err(InvalidOperationError::TooManyEntries);
        }

        let mut totals = HashMap::new();
        let mut preprocessed_entries = Vec::with_capacity(op.entries.len());
        for entry in op.entries {
            let account = accounts_in_scope
                .get(&entry.account)
                .ok_or(InvalidOperationError::AccountNotFound)?;

            let target_book = account
                .books
                .get(&(entry.currency, entry.balance_type))
                .ok_or(InvalidOperationError::BookNotFound)?;

            if entry.amount == 0 {
                return Err(InvalidOperationError::ZeroAmountEntry);
            }

            if !entry.ledger_code.is_ascii() || entry.ledger_code.len() > 8 {
                return Err(InvalidOperationError::LedgerCodeInvalid);
            }

            let mut ledger_code = [b' '; 8];
            let src = entry.ledger_code.as_bytes();
            ledger_code[8 - src.len()..].copy_from_slice(src);

            let currency_total = totals.entry(entry.currency).or_insert(0);

            // account nature agnostic zero-sum check
            *currency_total += match entry.op_type {
                AccountType::Debit => -entry.amount,
                AccountType::Credit => entry.amount,
            };

            // persisted amount sign flip
            let entry_amount = if account.account_type != entry.op_type {
                -entry.amount
            } else {
                entry.amount
            };

            preprocessed_entries.push(PreProcessedEntry {
                target_account_id: entry.account,
                target_book_id: *target_book,
                amount: entry_amount,
                ledger_code,
                currency: entry.currency,
                balance_type: entry.balance_type,
            });
        }
        if totals.iter().any(|(_, total)| *total != 0) {
            Err(InvalidOperationError::NonZeroSumEntries)
        } else {
            Ok(Self {
                idempotency_key: op.idempotency_key,
                accounts_in_scope,
                entries: preprocessed_entries,
            })
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum InvalidOperationError {
    #[error("too many entries in request")]
    TooManyEntries,
    #[error("account not found")]
    AccountNotFound,
    #[error("entry with zero amount is not allowed")]
    ZeroAmountEntry,
    #[error("entries are not DR/CR balanced")]
    NonZeroSumEntries,
    #[error("ledger code must be 8-character ASCII")]
    LedgerCodeInvalid,
    #[error("book not found")]
    BookNotFound,
}
