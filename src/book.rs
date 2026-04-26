use crate::AccountType;

struct Book {
    accounting_type: AccountType,
    cover_mirror: i32,
    durable_unapplied: Vec<BookRecord>,
    pending_write: Vec<BookRecord>,
}
struct BookRecord {
    accounting_type: AccountType,
    amount: i32,
    ledger_code: String,
    operation_id: String,
}

impl Book {
    fn apply_entry(&self, initial: i32, e: &BookRecord) -> i32 {
        match (e.accounting_type, self.accounting_type) {
            (AccountType::DEBIT, AccountType::DEBIT) => initial + e.amount,
            (AccountType::CREDIT, AccountType::DEBIT) => initial - e.amount,
            (AccountType::DEBIT, AccountType::CREDIT) => initial - e.amount,
            (AccountType::CREDIT, AccountType::CREDIT) => initial + e.amount,
        }
    }

    fn get_balance(&self) -> i32 {
        let durable_balance = self
            .durable_unapplied
            .iter()
            .fold(self.cover_mirror, |running: i32, e: &BookRecord| {
                self.apply_entry(running, e)
            });

        // TODO: verify if pending_write could have changed while folding durable_unapplied
        self.pending_write
            .iter()
            .fold(durable_balance, |running: i32, e: &BookRecord| {
                self.apply_entry(running, e)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_balance_debit() {
        let result = Book {
            accounting_type: AccountType::DEBIT,
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
            pending_write: vec![
                BookRecord {
                    accounting_type: AccountType::CREDIT,
                    amount: 200,
                    ledger_code: String::from("L3"),
                    operation_id: String::from("OP3"),
                },
                BookRecord {
                    accounting_type: AccountType::DEBIT,
                    amount: 150,
                    ledger_code: String::from("L2"),
                    operation_id: String::from("OP2"),
                },
            ],
        }
        .get_balance();
        assert_eq!(result, 0);
    }

    #[test]
    fn get_balance_credit() {
        let result = Book {
            accounting_type: AccountType::CREDIT,
            cover_mirror: 200,
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
            pending_write: vec![
                BookRecord {
                    accounting_type: AccountType::CREDIT,
                    amount: 200,
                    ledger_code: String::from("L3"),
                    operation_id: String::from("OP3"),
                },
                BookRecord {
                    accounting_type: AccountType::DEBIT,
                    amount: 150,
                    ledger_code: String::from("L2"),
                    operation_id: String::from("OP2"),
                },
            ],
        }
        .get_balance();
        assert_eq!(result, 300);
    }
}
