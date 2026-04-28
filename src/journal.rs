use crate::AccountType;
use std::time::SystemTime;

struct JournalRecord {
    operation_id: String,
    timestamp: SystemTime,
    write_entries: Vec<WriteEntry>,
}

struct WriteEntry {
    book_id: u32,
    page: u32,
    line: u16,
    accounting_type: AccountType,
    amount: i32,
}
