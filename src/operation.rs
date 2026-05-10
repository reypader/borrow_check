use crate::accounts::{AccountId, AccountState, AccountType};
use crate::books::{BalanceType, BookId, BookState, InScopeBook};
use crate::currency::Currency;
use crate::journal::{JournalEntryBytes, JournalHeaderBytes};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Deserialize, Debug)]
pub(crate) struct Operation {
    idempotency_key: Uuid,
    pub(crate) entries: Vec<OperationEntry>,
}

#[derive(Deserialize, Debug)]
pub(crate) struct OperationEntry {
    pub(crate) account: AccountId,
    balance_type: BalanceType,
    op_type: AccountType,
    amount: i128,
    currency: Currency,
    ledger_code: String,
}

#[derive(Debug)]
pub(crate) struct ValidOperation {
    pub(crate) idempotency_key: Uuid,
    pub(crate) entries_by_book: HashMap<BookId, Vec<PreProcessedEntry>>,
}

#[derive(Debug)]
pub(crate) struct PreProcessedEntry {
    pub(crate) target_account_id: AccountId,
    pub(crate) amount: i128,
    pub(crate) ledger_code: [u8; 8],
    pub(crate) currency: Currency,
    pub(crate) balance_type: BalanceType,
}

#[derive(Debug)]
pub(crate) struct PreparedOperation {
    pub(crate) idempotency_key: Uuid,
    pub(crate) total_entry_count: u8,
    pub(crate) record_length: u16,
    pub(crate) per_book: Vec<PreparedBook>,
}

#[derive(Debug)]
pub(crate) struct PreparedBook {
    pub(crate) book_id: BookId,
    pub(crate) account_id: AccountId,
    pub(crate) currency: Currency,
    pub(crate) balance_type: BalanceType,
    pub(crate) allow_overdraft: bool,
    pub(crate) state: Arc<RwLock<BookState>>,
    pub(crate) net_delta: i128,
    pub(crate) entries: Vec<PreProcessedEntry>,
}

impl ValidOperation {
    pub(crate) async fn parse(
        op: Operation,
        accounts_in_scope: &HashMap<AccountId, Arc<RwLock<AccountState>>>,
    ) -> Result<Self, InvalidOperationError> {
        //TODO make 100 configurable
        if op.entries.len() > 100 {
            return Err(InvalidOperationError::TooManyEntries);
        }

        let mut totals: HashMap<Currency, i128> = HashMap::new();
        let mut entries_by_book: HashMap<BookId, Vec<PreProcessedEntry>> =
            HashMap::with_capacity(op.entries.len());
        for entry in op.entries {
            let account = accounts_in_scope
                .get(&entry.account)
                .ok_or(InvalidOperationError::AccountNotFound)?
                .read()
                .await;

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

            entries_by_book
                .entry(*target_book)
                .or_default()
                .push(PreProcessedEntry {
                    target_account_id: entry.account,
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
                entries_by_book,
            })
        }
    }

    pub(crate) fn into_prepared(
        self,
        books_in_scope: &HashMap<BookId, InScopeBook>,
    ) -> Result<PreparedOperation, BookId> {
        let mut per_book = Vec::with_capacity(self.entries_by_book.len());
        let mut total_entries: usize = 0;

        for (book_id, group) in self.entries_by_book {
            let in_scope = books_in_scope.get(&book_id).ok_or(book_id)?;

            let net_delta: i128 = group.iter().map(|e| e.amount).sum();
            let head = &group[0];
            total_entries += group.len();

            per_book.push(PreparedBook {
                book_id,
                account_id: in_scope.account_id,
                currency: head.currency,
                balance_type: head.balance_type,
                allow_overdraft: in_scope.allow_overdraft,
                state: in_scope.state.clone(),
                net_delta,
                entries: group,
            });
        }

        let record_length = u16::try_from(
            size_of::<JournalHeaderBytes>() + size_of::<JournalEntryBytes>() * total_entries,
        )
        .expect("record length exceeded 65535");
        let total_entry_count = u8::try_from(total_entries).expect("entries exceeded 255");

        Ok(PreparedOperation {
            idempotency_key: self.idempotency_key,
            total_entry_count,
            record_length,
            per_book,
        })
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
