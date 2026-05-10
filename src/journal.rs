use crate::acceptor::{OperationError, OperationResult};
use crate::accounts::AccountId;
use crate::books::{BalanceType, BookState};
use crate::currency::Currency;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc, oneshot};
use zerocopy::byteorder::little_endian::{I128, U16, U32, U64};
use zerocopy::{Immutable, IntoBytes};

#[derive(IntoBytes, Immutable, Debug)]
#[repr(C)]
pub(crate) struct JournalCoverBytes {
    pub(crate) checkpoint_page: U32,
    pub(crate) checkpoint_line: U16,
    pub(crate) latest_page: U32,
    pub(crate) latest_line: U16,
    pub(crate) local_floor_page: U32,
    pub(crate) tip_operation_id: U64,
    pub(crate) tip_hash: [u8; 32],
}

#[derive(IntoBytes, Immutable, Debug)]
#[repr(C)]
pub(crate) struct JournalHeaderBytes {
    pub(crate) record_length: U16,
    pub(crate) entry_count: u8,
    pub(crate) operation_id: U64,
    pub(crate) timestamp_ns: U64,
    pub(crate) idempotency_key: [u8; 16],
    pub(crate) checksum: U32,
    pub(crate) prev_hash: [u8; 32],
}

#[derive(IntoBytes, Immutable, Debug)]
#[repr(C)]
pub(crate) struct JournalEntryBytes {
    pub(crate) target_book_id: U64,
    pub(crate) target_page: U32,
    pub(crate) target_line: U16,
    pub(crate) amount: I128,
    pub(crate) ledger_code: [u8; 8],
}

pub(crate) struct JournalWriter {
    //TODO make these private and instead create a constructor
    pub(crate) rx: mpsc::Receiver<WriterBuffer>,
    pub(crate) cover: JournalCoverBytes,
}

impl JournalWriter {
    pub(crate) async fn journal_writer_task(mut self) {
        //destructure JournalCoverBytes
        while let Some(mut write_buffer) = self.rx.recv().await {
            for pending in write_buffer.drain(..) {
                // check if current record would fit the current page
                // if not fill the page with 0s then repoint to the next page and write the header with the starting offset

                // start writing again.

                pending.ack().await;
            }
        }
    }
}

pub(crate) type WriterBuffer = Vec<WriteCommand>;

#[derive(Debug)]
pub(crate) struct WriteCommand {
    pub(crate) header: JournalHeaderBytes,
    pub(crate) book_contributions: Vec<BookContribution>,
    pub(crate) ending_balances: HashMap<(AccountId, Currency, BalanceType), i128>,
    pub(crate) response_tx: oneshot::Sender<Result<OperationResult, OperationError>>,
}

#[derive(Debug)]
pub(crate) struct BookContribution {
    pub(crate) state: Arc<RwLock<BookState>>,
    pub(crate) entries: Vec<JournalEntryBytes>,
}

impl WriteCommand {
    pub(crate) async fn abort(self, reason: OperationError) {
        for contribution in self.book_contributions {
            let n = contribution.entries.len();
            let mut guard = contribution.state.write().await;
            guard.pending_journal.drain(..n);
        }
        let _ = self.response_tx.send(Err(reason));
    }

    async fn ack(self) {
        for contribution in self.book_contributions {
            let n = contribution.entries.len();
            let mut guard = contribution.state.write().await;
            guard.pending_journal.drain(..n);
            guard.durable_pending_rollup.extend(contribution.entries);
        }
        let _ = self
            .response_tx
            .send(Ok(OperationResult::Accepted(self.ending_balances)));
    }
}
