use crate::{Account, AccountId, BalanceType, BookId, Currency, Operation};
use std::array::TryFromSliceError;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::io::{Cursor, Write};
use std::mem::replace;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::signal;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, sleep};
use uuid::Uuid;

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

#[derive(Debug)]
struct ProcessedOperation {
    idempotency_key: Uuid,
    entries: Vec<ProcessedEntry>,
    operation_id: u64,
    checksum: u32,
    prev_hash: [u8; 32],
}

#[derive(Debug)]
struct ProcessedEntry {
    target_book_id: BookId,
    amount: i128,
    ledger_code: [u8; 8],
    target_page: u32,
    target_line: u16,
}

impl ProcessedOperation {
    fn compacted(self) -> Result<Vec<u8>, WriteMismatchError> {
        let ProcessedOperation {
            idempotency_key,
            entries,
            checksum,
            prev_hash,
            operation_id,
        } = self;
        let record_length = 80 + (38 * entries.len());
        let mut result = Vec::with_capacity(record_length);
        let header = {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_nanos();
            let entry_count = entries.len() as u8; //safe, we'll be limiting the length at the entry point.
            let mut byte_array = [0u8; 80];
            let mut write_cursor = Cursor::new(&mut byte_array[..]);
            write_cursor.write_all(&(record_length as u16).to_le_bytes())?; //2
            write_cursor.write_all(&entry_count.to_le_bytes())?; //1
            write_cursor.write_all(&operation_id.to_le_bytes())?; //8
            write_cursor.write_all(&now.to_le_bytes())?; //16
            write_cursor.write_all(idempotency_key.as_bytes())?; //16
            write_cursor.write_all(&checksum.to_le_bytes())?; //4
            write_cursor.write_all(&prev_hash)?; //32
            write_cursor.write_all(&0u8.to_le_bytes())?; //8

            let is_filled = write_cursor.position() as usize == write_cursor.get_ref().len();
            if !is_filled {
                return Err(WriteMismatchError);
            } else {
                byte_array
            }
        };
        result.extend(header);

        for e in entries {
            let write_entry = {
                let ProcessedEntry {
                    target_book_id,
                    amount,
                    ledger_code,
                    target_page,
                    target_line,
                } = e;
                let mut byte_array = [0u8; 38];
                let mut write_cursor = Cursor::new(&mut byte_array[..]);
                write_cursor.write_all(&target_book_id.to_le_bytes())?; //8
                write_cursor.write_all(&target_page.to_le_bytes())?; //4
                write_cursor.write_all(&target_line.to_le_bytes())?; //2
                write_cursor.write_all(&amount.to_le_bytes())?; //16
                write_cursor.write_all(&ledger_code)?; //8

                let is_filled = write_cursor.position() as usize == write_cursor.get_ref().len();
                if !is_filled {
                    return Err(WriteMismatchError);
                } else {
                    byte_array
                }
            };
            result.extend(write_entry);
        }
        Ok(result)
    }
}

impl ValidOperation {
    pub fn parse(
        op: Operation,
        accounts_in_scope: HashMap<AccountId, Account>,
    ) -> Result<Self, InvalidOperationError> {
        let mut totals = HashMap::new();
        let mut preprocessed_entries = Vec::with_capacity(op.entries.len());
        for entry in op.entries {
            if entry.amount == 0 {
                return Err(InvalidOperationError::InvariantViolation);
            }

            if entry.ledger_code.len() > 8 {
                return Err(InvalidOperationError::InvariantViolation);
            }
            let ledger_code = format!("{: >8}", entry.ledger_code)
                .into_bytes()
                .as_slice()
                .try_into()?;

            let account = accounts_in_scope.get(&entry.account);

            match account {
                Some(account) => {
                    let entry_amount = if account.account_type != entry.op_type {
                        -entry.amount
                    } else {
                        entry.amount
                    };
                    let target_book = account.books.get(&(entry.currency, entry.balance_type));

                    match target_book {
                        Some(target_book_id) => {
                            preprocessed_entries.push(PreProcessedEntry {
                                target_account_id: entry.account,
                                target_book_id: *target_book_id,
                                amount: entry_amount,
                                ledger_code,
                                currency: entry.currency,
                                balance_type: entry.balance_type,
                            });
                        }
                        None => return Err(InvalidOperationError::BookNotFound),
                    }
                    let currency_total = totals.entry(entry.currency).or_insert(0);
                    *currency_total += entry_amount;
                }
                None => todo!(),
            }
        }
        if totals.iter().any(|(_, total)| *total != 0) {
            Err(InvalidOperationError::InvariantViolation)
        } else {
            Ok(Self {
                idempotency_key: op.idempotency_key,
                entries: preprocessed_entries,
            })
        }
    }
}

#[derive(Debug, Error)]
pub struct WriteMismatchError;

impl From<std::io::Error> for WriteMismatchError {
    fn from(e: std::io::Error) -> Self {
        println!("Error while compacting {}", e);
        WriteMismatchError
    }
}

impl Display for WriteMismatchError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        //TODO better error info
        f.write_str("Data size didn't match target serialization format")
    }
}
#[derive(Debug, Error)]
pub enum InvalidOperationError {
    InvariantViolation,
    BookNotFound,
}

impl Display for InvalidOperationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        //TODO better error info
        f.write_str("Invalid operation structure")
    }
}

impl From<TryFromSliceError> for InvalidOperationError {
    fn from(_: TryFromSliceError) -> Self {
        InvalidOperationError::InvariantViolation
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
        loop {
            tokio::select! {
                Some(mut write_buffer) = self.rx.recv() => {
                    //TODO actually write to file
                    for pending in write_buffer.drain(..) {
                        pending.ack()
                    }
                }
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
        let mut processed_entries = Vec::with_capacity(entries.capacity());
        let totals = entries.iter().fold(HashMap::new(), |mut t, entry| {
            // TODO: identify book_id, target_page, target_lin
            processed_entries.push(ProcessedEntry {
                target_book_id: 0u64,
                amount: entry.amount,
                ledger_code: [0; 8],
                target_page: 0u32,
                target_line: 0u16,
            });
            let running_total = t.entry(entry.target_account_id).or_insert((
                entry.currency,
                entry.balance_type,
                0i128,
            ));
            running_total.2 += entry.amount;
            t
        });

        //TODO re-validate balances against totals then send OperationResult::Rejected

        // TODO: identify checksum, prev_hash, operation_id
        let processed = ProcessedOperation {
            idempotency_key: operations.idempotency_key,
            entries: processed_entries,
            operation_id: 0u64,
            checksum: 0u32,
            prev_hash: [0; 32],
        };

        let compaction_result = processed.compacted();
        match compaction_result {
            Ok(content) => {
                self.buffer.push(WriteCommand {
                    content,
                    ending_balances: totals, //TODO replace with computed ending balance
                    response_tx,
                });
            }
            Err(_) => {
                println!("WriteMismatch");
                let _ = response_tx.send(Err(OperationError::WriterSystemFailure));
            }
        }
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
    content: Vec<u8>,
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
