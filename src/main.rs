#[macro_use]
extern crate rocket;

use crate::acceptor_task::OperationResult::{Accepted, Rejected};
use crate::acceptor_task::{Acceptor, AcceptorHandle, ValidOperation, Writer};
use crate::ApiError::{SystemError, Unprocessable};
use rocket::serde::json::Json;
use rocket::State;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

mod acceptor_task;
type AccountId = u32;

#[derive(Deserialize, Serialize, Copy, Clone, Debug)]
enum AccountType {
    DEBIT,
    CREDIT,
}

#[derive(Deserialize, Serialize, Eq, Hash, PartialEq, Copy, Clone, Debug)]
pub enum BalanceType {
    Current,
    Available,
    Hold,
}

#[derive(Deserialize, Debug)]
struct Operation {
    entries: Vec<OperationEntry>,
}

#[derive(Deserialize, Debug)]
struct OperationEntry {
    account: AccountId,
    balance_type: BalanceType,
    op_type: AccountType,
    amount: i128,
}

#[derive(Serialize)]
struct OperationResult {
    resulting_balances: Vec<BalanceDescriptor>,
}

#[derive(Serialize)]
struct BalanceDescriptor {
    account: u32,
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

#[post("/operations", format = "application/json", data = "<operation>")]
async fn post_operations(
    operation: Json<Operation>,
    acceptor_handle: &State<AcceptorHandle>, //This shouldn't be
) -> Result<Json<OperationResult>, ApiError> {
    //TODO react to validation error
    match acceptor_handle
        .submit(ValidOperation::parse(operation.0).unwrap())
        .await
    {
        Ok(Accepted(balances)) => Ok(Json(OperationResult {
            //TODO refactor this to a better mapping
            resulting_balances: balances
                .into_iter()
                .map(|x| BalanceDescriptor {
                    account: x.0,
                    balance_type: BalanceType::Current,
                    balance: x.1,
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
    let (writer_tx, writer_rx) = mpsc::channel(5);
    let writer = Writer { rx: writer_rx };
    tokio::spawn(writer.writer_task());

    let (acceptor_tx, acceptor_rx) = mpsc::channel(5);
    let acceptor = Acceptor {
        buffer: Vec::with_capacity(10),
        writer_tx,
        rx: acceptor_rx,
    };
    tokio::spawn(acceptor.acceptor_task());
    let _ = rocket::build()
        .manage(AcceptorHandle { tx: acceptor_tx })
        .mount("/", routes![post_operations])
        .launch()
        .await;
}
