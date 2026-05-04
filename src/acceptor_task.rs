use crate::{Account, AccountId, BalanceType, BookId, Currency, Operation};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::mem::replace;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::signal;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, sleep};
use uuid::Uuid;
use zerocopy::byteorder::little_endian::{I128, U16, U32, U64, U128};
use zerocopy::{Immutable, IntoBytes};

pub fn spawn(
    buffer_capacity: usize,
    channel_capacity: usize,
    manual_flush_after: Duration,
) -> AcceptorHandle {
    let (writer_tx, writer_rx) = mpsc::channel(channel_capacity);
    let writer = Writer { rx: writer_rx };
    tokio::spawn(writer.writer_task());

    let (acceptor_tx, acceptor_rx) = mpsc::channel(channel_capacity);
    let acceptor = Acceptor {
        buffer: Vec::with_capacity(buffer_capacity),
        writer_tx,
        rx: acceptor_rx,
    };
    tokio::spawn(acceptor.acceptor_task(manual_flush_after));
    AcceptorHandle { tx: acceptor_tx }
}

#[derive(Debug)]
pub struct ValidOperation {
    idempotency_key: Uuid,
    entries: Vec<PreProcessedEntry>,
}

#[derive(Debug)]
struct PreProcessedEntry {
    target_account_id: AccountId,
    target_book_id: BookId,
    amount: i128,
    ledger_code: [u8; 8],
    currency: Currency,
    balance_type: BalanceType,
}

#[derive(IntoBytes, Immutable, Debug)]
#[repr(C)]
struct HeaderRecord {
    record_length: U16,
    entry_count: u8,
    operation_id: U64,
    timestamp_ns: U128,
    idempotency_key: [u8; 16],
    checksum: U32,
    prev_hash: [u8; 32],
    _pad: u8,
}

#[derive(IntoBytes, Immutable, Debug)]
#[repr(C)]
struct EntryRecord {
    target_book_id: U64,
    target_page: U32,
    target_line: U16,
    amount: I128,
    ledger_code: [u8; 8],
}

impl ValidOperation {
    pub fn parse(
        op: Operation,
        accounts_in_scope: HashMap<AccountId, Account>,
    ) -> Result<Self, InvalidOperationError> {
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

            let entry_amount = if account.account_type != entry.op_type {
                -entry.amount
            } else {
                entry.amount
            };
            let currency_total = totals.entry(entry.currency).or_insert(0);
            *currency_total += entry_amount;

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
                entries: preprocessed_entries,
            })
        }
    }
}

#[derive(Debug, Error)]
pub enum InvalidOperationError {
    AccountNotFound,
    ZeroAmountEntry,
    NonZeroSumEntries,
    LedgerCodeInvalid,
    BookNotFound,
}

impl Display for InvalidOperationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        //TODO better error info
        f.write_str("Invalid operation structure")
    }
}

impl From<mpsc::error::SendError<WriterBuffer>> for OperationError {
    fn from(_: mpsc::error::SendError<WriterBuffer>) -> Self {
        //TODO propagate better context?
        OperationError::WriterSystemFailure
    }
}
impl From<oneshot::error::RecvError> for OperationError {
    fn from(_: oneshot::error::RecvError) -> Self {
        //TODO propagate better context?
        OperationError::AcceptorSystemFailure
    }
}
struct Writer {
    //TODO make these private and instead create a constructor
    rx: mpsc::Receiver<WriterBuffer>,
}

impl Writer {
    async fn writer_task(mut self) {
        while let Some(mut write_buffer) = self.rx.recv().await {
            for pending in write_buffer.drain(..) {
                pending.ack();
            }
        }
    }
}

type WriterBuffer = Vec<WriteCommand>;
struct Acceptor {
    //TODO make these private and instead create a constructor
    buffer: WriterBuffer,
    writer_tx: mpsc::Sender<WriterBuffer>,
    rx: mpsc::Receiver<AcceptorCommand>,
}

impl Acceptor {
    async fn handle(&mut self, cmd: AcceptorCommand) {
        let AcceptorCommand {
            operations,
            response_tx,
        } = cmd;
        let entries = &operations.entries;
        let mut processed_entries = Vec::with_capacity(entries.len());
        let mut totals = HashMap::new();
        for entry in entries {
            // TODO: identify book_id, target_page, target_lin
            let target_book_id = 0;
            let target_page = 0;
            let target_line = 0;

            processed_entries.push(EntryRecord {
                target_book_id: target_book_id.into(),
                target_page: target_page.into(),
                target_line: target_line.into(),
                amount: entry.amount.into(),
                ledger_code: entry.ledger_code,
            });
            let running_total = totals.entry(entry.target_account_id).or_insert((
                entry.currency,
                entry.balance_type,
                0i128,
            ));
            running_total.2 += entry.amount;
        }

        //TODO re-validate balances against totals then send OperationResult::Rejected
        let ending_balances = totals;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos();
        let entry_count = entries.len() as u8; //safe, we'll be limiting the length at the entry point.
        let record_length = 80 + (38 * entries.len()) as u16;

        // TODO: identify checksum, prev_hash, operation_id
        let operation_id = 0;
        let checksum = 0;
        let prev_hash = [0; 32];

        let header = HeaderRecord {
            record_length: record_length.into(),
            entry_count: entry_count,
            operation_id: operation_id.into(),
            timestamp_ns: now.into(),
            idempotency_key: operations.idempotency_key.into_bytes(),
            checksum: checksum.into(),
            prev_hash,
            _pad: 0,
        };

        self.buffer.push(WriteCommand {
            header,
            entries: processed_entries,
            ending_balances,
            response_tx,
        });
    }

    async fn flush(&mut self) {
        let buffer_size = self.buffer.capacity();
        let buffer_to_flush = replace(&mut self.buffer, Vec::with_capacity(buffer_size));
        match self.writer_tx.send(buffer_to_flush).await {
            Ok(_) => (),
            Err(_) => {
                self.rx.close();
                while let Some(in_flight) = self.rx.recv().await {
                    in_flight.abort()
                }
                for pending in self.buffer.drain(..) {
                    pending.abort(OperationError::WriterSystemFailure)
                }
            }
        }
    }

    async fn acceptor_task(mut self, manual_flush_after: Duration) {
        // Start receiving messages
        let flush_timer = sleep(manual_flush_after);
        tokio::pin!(flush_timer);
        loop {
            tokio::select! {
                biased; // always prioritize trying to "receive" a message
                Some(cmd) = self.rx.recv() => {
                    let buffer_was_empty = self.buffer.is_empty();

                    // TODO assess handle returning boolean vs self.buffer.is_empty()
                    self.handle(cmd).await;

                    if buffer_was_empty && !self.buffer.is_empty() {
                        flush_timer.as_mut().reset(Instant::now() + manual_flush_after);
                    }

                    if self.buffer.len() == self.buffer.capacity() {
                        //TODO use standard logging
                        println!("Full-flushing {}", self.buffer.len());
                       self.flush().await;
                    }
                }

                _  = &mut flush_timer, if !self.buffer.is_empty() => {
                    //TODO use standard logging
                     println!("Timed flushing {}", self.buffer.len());
                     self.flush().await;
                }

                //TODO Add other shutdown signals?
                _ = signal::ctrl_c()  => {
                    if !self.buffer.is_empty() {
                        self.flush().await;
                    }
                    break;
                }

                else => break,
            }
        }
    }
}
#[derive(Debug, Error)]
pub enum OperationError {
    AcceptorSystemFailure,
    WriterSystemFailure,
}

impl From<mpsc::error::SendError<AcceptorCommand>> for OperationError {
    fn from(_: mpsc::error::SendError<AcceptorCommand>) -> Self {
        //TODO propagate better context?
        OperationError::AcceptorSystemFailure
    }
}
impl Display for OperationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationError::AcceptorSystemFailure => f.write_str("acceptor is no longer running"),
            OperationError::WriterSystemFailure => f.write_str("writer is no longer running"),
        }
    }
}

#[derive(Debug)]
struct AcceptorCommand {
    operations: ValidOperation,
    response_tx: oneshot::Sender<Result<OperationResult, OperationError>>,
}

impl AcceptorCommand {
    fn abort(self) {
        let _ = self
            .response_tx
            .send(Err(OperationError::WriterSystemFailure));
    }
}

#[derive(Clone)]
pub struct AcceptorHandle {
    //TODO make these private and instead create a constructor
    tx: mpsc::Sender<AcceptorCommand>,
}

impl AcceptorHandle {
    pub async fn submit(&self, op: ValidOperation) -> Result<OperationResult, OperationError> {
        let (response_sender, response_receiver) = oneshot::channel();
        self.tx
            .send(AcceptorCommand {
                operations: op,
                response_tx: response_sender,
            })
            .await?;
        response_receiver.await?
    }
}
#[derive(Debug)]
pub enum OperationResult {
    /// Contains the resulting balances after the accepted operation
    Accepted(HashMap<AccountId, (Currency, BalanceType, i128)>),

    /// Contains the AccountId whose balance caused the rejection
    Rejected(AccountId),
}

#[derive(Debug)]
struct WriteCommand {
    header: HeaderRecord,
    entries: Vec<EntryRecord>,
    ending_balances: HashMap<AccountId, (Currency, BalanceType, i128)>,
    response_tx: oneshot::Sender<Result<OperationResult, OperationError>>,
}

impl WriteCommand {
    fn abort(self, reason: OperationError) {
        let _ = self.response_tx.send(Err(reason));
    }

    fn ack(self) {
        let _ = self
            .response_tx
            .send(Ok(OperationResult::Accepted(self.ending_balances)));
    }
}
