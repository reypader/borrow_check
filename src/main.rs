#[macro_use]
extern crate rocket;

use crate::book::{Book, BookRecord};
use rocket::serde::json::Json;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

mod book;
mod journal;

#[derive(Deserialize, Serialize, Copy, Clone)]
enum AccountType {
    DEBIT,
    CREDIT,
}

#[derive(Deserialize)]
struct Operation {
    entries: Vec<OperationEntry>,
}

#[derive(Deserialize)]
struct OperationEntry {
    account: u32,
    balance_type: BalanceType,
    op_type: AccountType,
    amount: i32,
}

#[derive(Deserialize, Serialize, Eq, Hash, PartialEq, Copy, Clone)]
pub enum BalanceType {
    Current,
    Available,
    Hold,
}

#[derive(Serialize)]
struct OperationResult {
    resulting_balances: Vec<BalanceDescriptor>,
}

#[derive(Serialize)]
struct BalanceDescriptor {
    account: u32,
    balance_type: BalanceType,
    balance: i32,
}

#[post("/operations", format = "application/json", data = "<operation>")]
fn post_operations(operation: Json<Operation>) -> Json<OperationResult> {
    let mut book_registry = HashMap::with_capacity(2);
    book_registry.insert(
        (1u32, BalanceType::Available),
        Book { //actually starts at 150
            accounting_type: AccountType::CREDIT,
            cover_mirror: 100,
            durable_unapplied: vec![
                BookRecord {
                    accounting_type: AccountType::DEBIT,
                    amount: 100,
                    ledger_code: String::from("L1"),
                    operation_id: String::from("OP1"),
                },
                BookRecord {
                    accounting_type: AccountType::CREDIT,
                    amount: 150,
                    ledger_code: String::from("L4"),
                    operation_id: String::from("OP4"),
                },
            ],
            pending_write: vec![],
        },
    );

    book_registry.insert(
        (2u32, BalanceType::Available),
        Book { //actually starts at 250
            accounting_type: AccountType::CREDIT,
            cover_mirror: 100,
            durable_unapplied: vec![
                BookRecord {
                    accounting_type: AccountType::DEBIT,
                    amount: 100,
                    ledger_code: String::from("L1"),
                    operation_id: String::from("OP1"),
                },
                BookRecord {
                    accounting_type: AccountType::CREDIT,
                    amount: 250,
                    ledger_code: String::from("L4"),
                    operation_id: String::from("OP4"),
                },
            ],
            pending_write: vec![],
        },
    );
    let mut operation_totals = HashMap::with_capacity(operation.entries.len());
    operation.entries.iter().for_each(|entry| {
        let debit_credit = operation_totals.entry(entry.account).or_insert((0, 0));
        *debit_credit = match entry.op_type {
            AccountType::DEBIT => (debit_credit.0 + entry.amount, debit_credit.1),
            AccountType::CREDIT => (debit_credit.0, debit_credit.1 + entry.amount),
        };
        let book = (entry.account, entry.balance_type);
        match book_registry.get_mut(&book) {
            None => panic!("No registered book found"),
            Some(b) => b.pending_write.push(BookRecord {
                accounting_type: entry.op_type,
                amount: entry.amount,
                ledger_code: "L1".to_string(),
                operation_id: "OP1".to_string(),
            }),
        };
    });

    let mut resulting_balances = Vec::new();
    for x in book_registry {
        resulting_balances.push(BalanceDescriptor {
            account: x.0.0,
            balance_type: x.0.1,
            balance: x.1.get_balance(),
        });
    }
    Json(OperationResult { resulting_balances })
}

#[launch]
fn rocket() -> _ {
    rocket::build().mount("/", routes![post_operations])
}
