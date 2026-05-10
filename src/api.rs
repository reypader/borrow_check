use crate::acceptor::OperationResult::{Accepted, Overflow, Rejected};
use crate::acceptor::SystemHandles;
use crate::accounts::AccountId;
use crate::books::{BalanceType, InScopeBook};
use crate::currency::Currency;
use crate::operation::{InvalidOperationError, Operation, ValidOperation};
use ApiError::{SystemError, Unprocessable};
use rocket::State;
use rocket::serde::json::Json;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Serialize)]
pub(crate) struct OperationResult {
    resulting_balances: HashMap<AccountId, Vec<BalanceDescriptor>>,
}

#[derive(Serialize)]
struct BalanceDescriptor {
    currency: Currency,
    balance_type: BalanceType,
    balance: i128,
}

#[derive(Responder)]
pub(crate) enum ApiError {
    #[response(status = 400)]
    BadRequest(String),
    #[response(status = 404)]
    NotFound(String),
    #[response(status = 422)]
    Unprocessable(String),
    #[response(status = 500)]
    SystemError(String),
}

impl From<InvalidOperationError> for ApiError {
    fn from(_value: InvalidOperationError) -> Self {
        //TODO add more info
        //TODO "to_string" is good?
        ApiError::BadRequest("request validation failed".to_string())
    }
}

#[post("/operations", format = "application/json", data = "<operation>")]
pub(crate) async fn post_operations(
    operation: Json<Operation>,
    system_handle: &State<SystemHandles>, //This shouldn't be
) -> Result<Json<OperationResult>, ApiError> {
    let op = operation.0;

    let account_ids = op.entries.iter().map(|e| e.account).collect();
    let loaded_accounts = system_handle.load_accounts(account_ids).await.unwrap(); //TODO surface account load error
    let valid_op = ValidOperation::parse(op, &loaded_accounts).await?;

    let books_in_scope = {
        let book_to_account: HashMap<u64, u64> = valid_op
            .entries_by_book
            .iter()
            .map(|(book_id, entries)| (*book_id, entries[0].target_account_id))
            .collect();
        let book_ids = book_to_account.keys().copied().collect();
        let loaded_books = system_handle.load_books(book_ids).await.unwrap(); //TODO surface book load error
        let mut result = HashMap::with_capacity(loaded_books.len());
        for (book_id, state) in loaded_books {
            let account_id = book_to_account[&book_id];
            let allow_overdraft = loaded_accounts[&account_id].read().await.allow_overdraft;
            result.insert(
                book_id,
                InScopeBook {
                    account_id,
                    allow_overdraft,
                    state,
                },
            );
        }
        result
    };

    let prepared = valid_op
        .into_prepared(&books_in_scope)
        .map_err(|book_id| Unprocessable(format!("Book {:?} not found", book_id)))?;

    match system_handle.submit_operation(prepared).await {
        Ok(Accepted(balances)) => {
            let mut resulting_balances: HashMap<AccountId, Vec<BalanceDescriptor>> = HashMap::new();
            for ((account_id, currency, balance_type), balance) in balances {
                resulting_balances
                    .entry(account_id)
                    .or_default()
                    .push(BalanceDescriptor {
                        currency,
                        balance_type,
                        balance,
                    });
            }
            Ok(Json(OperationResult { resulting_balances }))
        }
        Ok(Rejected(offending_account_id)) => Err(Unprocessable(format!(
            "Account {:?} does not have enough balance",
            offending_account_id
        ))),
        Ok(Overflow(offending_book_id)) => Err(Unprocessable(format!(
            "Book {:?} balance would exceed representable range",
            offending_book_id
        ))),
        Err(x) => Err(SystemError(format!("system error: {:?}", x))),
    }
}
