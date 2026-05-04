#[macro_use]
extern crate rocket;

use std::collections::HashMap;
use std::time::Duration;

use crate::ApiError::{SystemError, Unprocessable};
use crate::acceptor_task::OperationResult::{Accepted, Rejected};
use crate::acceptor_task::{AcceptorHandle, InvalidOperationError, ValidOperation};
use rocket::State;
use rocket::serde::json::Json;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

mod acceptor_task;
type AccountId = u64;
type BookId = u64;
type Currency = u16;
#[derive(Deserialize, Serialize, Copy, Clone, Debug, Eq, PartialEq)]
enum AccountType {
    Debit,
    Credit,
}

#[derive(Deserialize, Serialize, Eq, Hash, PartialEq, Copy, Clone, Debug)]
pub enum BalanceType {
    Current,
    Available,
    Hold,
}

#[derive(Deserialize, Debug)]
struct Operation {
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

#[derive(Serialize)]
struct OperationResult {
    resulting_balances: Vec<BalanceDescriptor>,
}

#[derive(Serialize)]
struct BalanceDescriptor {
    account: u64,
    currency: Currency,
    balance_type: BalanceType,
    balance: i128,
}

#[derive(Responder)]
enum ApiError {
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

#[derive(Debug)]
struct Account {
    account_type: AccountType,
    books: HashMap<(Currency, BalanceType), BookId>,
}

#[post("/operations", format = "application/json", data = "<operation>")]
async fn post_operations(
    operation: Json<Operation>,
    acceptor_handle: &State<AcceptorHandle>, //This shouldn't be
) -> Result<Json<OperationResult>, ApiError> {
    //TODO pre-load accounts
    let loaded_accounts = HashMap::from([
        (
            1,
            Account {
                account_type: AccountType::Credit,
                books: HashMap::from([
                    ((123, BalanceType::Available), 1),
                    ((123, BalanceType::Current), 2),
                    ((123, BalanceType::Hold), 3),
                ]),
            },
        ),
        (
            2,
            Account {
                account_type: AccountType::Credit,
                books: HashMap::from([
                    ((123, BalanceType::Available), 1),
                    ((123, BalanceType::Current), 2),
                    ((123, BalanceType::Hold), 3),
                ]),
            },
        ),
    ]);
    let valid_op = ValidOperation::parse(operation.0, loaded_accounts)?;
    match acceptor_handle.submit(valid_op).await {
        Ok(Accepted(balances)) => Ok(Json(OperationResult {
            //TODO refactor this to a better mapping
            resulting_balances: balances
                .into_iter()
                .map(|x| BalanceDescriptor {
                    account: x.0,
                    currency: x.1.0,
                    balance_type: x.1.1,
                    balance: x.1.2,
                })
                .collect(),
        })),
        Ok(Rejected(offending_account_id)) => Err(Unprocessable(format!(
            "Account {:?} does not have enough balance",
            offending_account_id
        ))),
        Err(x) => Err(SystemError(format!("system error: {:?}", x))),
    }
}

#[rocket::main]
async fn main() {
    let acceptor_handle = acceptor_task::spawn(10, 5, Duration::from_millis(10));
    let _ = rocket::build()
        .manage(acceptor_handle)
        .mount("/", routes![post_operations])
        .launch()
        .await;
}
