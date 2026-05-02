use crate::{AccountId, AccountType, Operation};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::mem::replace;
use thiserror::Error;
use tokio::signal;
use tokio::sync::{mpsc, oneshot};

pub fn spawn(buffer_capacity: usize, channel_capacity: usize) -> AcceptorHandle {
    let (writer_tx, writer_rx) = mpsc::channel(channel_capacity);
    let writer = Writer { rx: writer_rx };
    tokio::spawn(writer.writer_task());

    let (acceptor_tx, acceptor_rx) = mpsc::channel(channel_capacity);
    let acceptor = Acceptor {
        buffer: Vec::with_capacity(buffer_capacity),
        writer_tx,
        rx: acceptor_rx,
    };
    tokio::spawn(acceptor.acceptor_task());
    AcceptorHandle { tx: acceptor_tx }
}

#[derive(Debug)]
pub struct ValidOperation(Operation);

impl ValidOperation {
    pub fn parse(op: Operation) -> Result<Self, InvalidOperationError> {
        // TODO validate amounts, zero-sum
        Ok(Self(op))
    }
}

#[derive(Debug, Error)]
pub struct InvalidOperationError;

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
        let entries = cmd.operations.0.entries;
        let totals = entries.iter().fold(HashMap::new(), |mut t, entry| {
            let running_total = t.entry(entry.account).or_insert(0i128);
            *running_total += match entry.op_type {
                //TODO consider account/book's accounting type
                AccountType::DEBIT => -entry.amount,
                AccountType::CREDIT => entry.amount,
            };
            t
        });

        //TODO re-validate balances
        //TODO send OperationResult::Rejected
        //TODO construct content string

        let content = "".to_string();
        self.buffer.push(WriteCommand {
            content,
            ending_balances: totals, //TODO replace with computed ending balance
            response_tx: cmd.response_tx,
        });
    }

    async fn flush(&mut self) {
        let buffer_size = self.buffer.len();
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

    async fn acceptor_task(mut self) {
        // Start receiving messages
        loop {
            tokio::select! {
                Some(cmd) = self.rx.recv() => {
                    self.handle(cmd).await;

                    //TODO reset manual flushing schedule

                    if self.buffer.len() == 10 {
                       self.flush().await;
                    }
                }

                //TODO scheduled manual flush

                //TODO Add other shutdown signals?
                _ =signal::ctrl_c()  => {
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
    Accepted(HashMap<AccountId, i128>),

    /// Contains the AccountId whose balance caused the rejection
    Rejected(AccountId),
}

#[derive(Debug)]
struct WriteCommand {
    content: String,
    ending_balances: HashMap<AccountId, i128>,
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
