use crate::accounts::{
    AccountId, AccountLoadError, AccountRegistryActor, AccountRegistryCommand, AccountRegistryMap,
    AccountState,
};
use crate::books::{
    BalanceType, BookId, BookLoadError, BookRegistryActor, BookRegistryCommand, BookRegistryMap,
    BookState,
};
use crate::currency::Currency;
use crate::journal::{
    BookContribution, JournalCoverBytes, JournalEntryBytes, JournalHeaderBytes, JournalWriter,
    WriteCommand, WriterBuffer,
};
use crate::operation::{PreparedBook, PreparedOperation};
use std::collections::HashMap;
use std::mem::replace;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::signal;
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio::time::{Instant, sleep};

pub(crate) fn spawn(
    buffer_capacity: usize,
    channel_capacity: usize,
    manual_flush_after: Duration,
) -> SystemHandles {
    let (account_registry_tx, account_registry_rx) = mpsc::channel(channel_capacity);

    let account_registry = AccountRegistryActor {
        map: AccountRegistryMap::default(),
        rx: account_registry_rx,
    };

    tokio::spawn(account_registry.account_registry_task());

    let (book_registry_tx, book_registry_rx) = mpsc::channel(channel_capacity);

    let book_registry = BookRegistryActor {
        map: BookRegistryMap::default(),
        rx: book_registry_rx,
    };

    tokio::spawn(book_registry.book_registry_task());

    let (writer_tx, writer_rx) = mpsc::channel(channel_capacity);

    //TODO pre-load cover from disk
    let cover = JournalCoverBytes {
        checkpoint_page: 0u32.into(),
        checkpoint_line: 0u16.into(),
        latest_page: 0u32.into(),
        latest_line: 0u16.into(),
        local_floor_page: 0u32.into(),
        tip_operation_id: 0u64.into(),
        tip_hash: [0; 32],
    };

    //TODO start pointing to current page then fill with zeros then point to the next page

    let writer = JournalWriter {
        rx: writer_rx,
        cover,
    };
    tokio::spawn(writer.journal_writer_task());

    let (acceptor_tx, acceptor_rx) = mpsc::channel(channel_capacity);
    let acceptor = Acceptor {
        buffer: Vec::with_capacity(buffer_capacity),
        writer_tx,
        rx: acceptor_rx,
    };
    tokio::spawn(acceptor.acceptor_task(manual_flush_after));
    SystemHandles {
        acceptor_tx,
        account_tx: account_registry_tx,
        book_tx: book_registry_tx,
    }
}

struct Acceptor {
    //TODO make these private and instead create a constructor
    buffer: WriterBuffer,
    writer_tx: mpsc::Sender<WriterBuffer>,
    rx: mpsc::Receiver<AcceptorCommand>,
}

impl Acceptor {
    async fn handle(&mut self, cmd: AcceptorCommand) {
        let AcceptorCommand {
            prepared,
            response_tx,
        } = cmd;
        let PreparedOperation {
            idempotency_key,
            total_entry_count,
            record_length,
            per_book,
        } = prepared;

        let mut ending_balances: HashMap<(AccountId, Currency, BalanceType), i128> =
            HashMap::with_capacity(per_book.len());

        for book in &per_book {
            let guard = book.state.read().await;
            let durable =
                guard
                    .durable_pending_rollup
                    .iter()
                    .fold(guard.running_balance, |acc, r| {
                        let val: i128 = r.amount.into();
                        acc + val
                    });
            let merged = guard
                .pending_journal
                .iter()
                .fold(durable, |acc, &amt| acc + amt);
            drop(guard);

            let projected = match merged.checked_add(book.net_delta) {
                Some(v) => v,
                None => {
                    let _ = response_tx.send(Ok(OperationResult::Overflow(book.book_id)));
                    return;
                }
            };

            if projected < 0 && !book.allow_overdraft {
                let _ = response_tx.send(Ok(OperationResult::Rejected(book.account_id)));
                return;
            }

            ending_balances.insert(
                (book.account_id, book.currency, book.balance_type),
                projected,
            );
        }

        let mut book_contributions: Vec<BookContribution> = Vec::with_capacity(per_book.len());
        for book in per_book {
            let PreparedBook {
                book_id,
                state,
                entries,
                ..
            } = book;
            let mut journal_entries: Vec<JournalEntryBytes> = Vec::with_capacity(entries.len());
            let mut guard = state.write().await;
            for entry in &entries {
                guard.pending_journal.push_back(entry.amount);
                // TODO: writer assigns target_page, target_line
                journal_entries.push(JournalEntryBytes {
                    target_book_id: book_id.into(),
                    target_page: 0u32.into(),
                    target_line: 0u16.into(),
                    amount: entry.amount.into(),
                    ledger_code: entry.ledger_code,
                });
            }
            drop(guard);
            book_contributions.push(BookContribution {
                state,
                entries: journal_entries,
            });
        }

        let nanos_u128: u128 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before 1970")
            .as_nanos();
        let nanos: u64 = u64::try_from(nanos_u128).expect("timestamp past year 2554");

        // TODO: identify checksum, prev_hash, operation_id
        let operation_id = 0;
        let checksum = 0;
        let prev_hash = [0; 32];

        let header = JournalHeaderBytes {
            record_length: record_length.into(),
            entry_count: total_entry_count,
            operation_id: operation_id.into(),
            timestamp_ns: nanos.into(),
            idempotency_key: idempotency_key.into_bytes(),
            checksum: checksum.into(),
            prev_hash,
        };

        self.buffer.push(WriteCommand {
            header,
            book_contributions,
            ending_balances,
            response_tx,
        });
    }

    async fn flush(&mut self) {
        let buffer_size = self.buffer.capacity();
        let buffer_to_flush = replace(&mut self.buffer, Vec::with_capacity(buffer_size));
        match self.writer_tx.send(buffer_to_flush).await {
            Ok(_) => (),
            Err(mpsc::error::SendError(mut failed_commands)) => {
                self.rx.close();
                while let Some(in_flight) = self.rx.recv().await {
                    in_flight.abort()
                }
                for pending in failed_commands.drain(..) {
                    pending.abort(OperationError::AbortedFromFailedFlush).await
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
pub(crate) enum OperationError {
    #[error("acceptor is no longer running")]
    AcceptorSystemFailure,
    #[error("operation aborted due to failed flush")]
    AbortedFromFailedFlush,
    #[error("acceptor dropped response channel")]
    Recv(#[from] oneshot::error::RecvError),
}

#[derive(Debug)]
struct AcceptorCommand {
    prepared: PreparedOperation,
    response_tx: oneshot::Sender<Result<OperationResult, OperationError>>,
}

impl AcceptorCommand {
    fn abort(self) {
        let _ = self
            .response_tx
            .send(Err(OperationError::AbortedFromFailedFlush));
    }
}

#[derive(Clone)]
pub(crate) struct SystemHandles {
    acceptor_tx: mpsc::Sender<AcceptorCommand>,
    account_tx: mpsc::Sender<AccountRegistryCommand>,
    book_tx: mpsc::Sender<BookRegistryCommand>,
}

impl SystemHandles {
    pub(crate) async fn submit_operation(
        &self,
        prepared: PreparedOperation,
    ) -> Result<OperationResult, OperationError> {
        let (response_sender, response_receiver) = oneshot::channel();
        self.acceptor_tx
            .send(AcceptorCommand {
                prepared,
                response_tx: response_sender,
            })
            .await
            .map_err(|_| OperationError::AcceptorSystemFailure)?;
        response_receiver.await?
    }

    pub(crate) async fn load_accounts(
        &self,
        ids: Vec<AccountId>,
    ) -> Result<HashMap<AccountId, Arc<RwLock<AccountState>>>, AccountLoadError> {
        let (response_sender, response_receiver) = oneshot::channel();
        self.account_tx
            .send(AccountRegistryCommand::GetAccounts {
                ids,
                reply: response_sender,
            })
            .await
            .map_err(|_| AccountLoadError::AccountNotFound)?;
        response_receiver.await?
    }

    pub(crate) async fn load_books(
        &self,
        ids: Vec<BookId>,
    ) -> Result<HashMap<BookId, Arc<RwLock<BookState>>>, BookLoadError> {
        let (response_sender, response_receiver) = oneshot::channel();
        self.book_tx
            .send(BookRegistryCommand::GetBooks {
                ids,
                reply: response_sender,
            })
            .await
            .map_err(|_| BookLoadError::BookNotFound)?;
        response_receiver
            .await
            .map_err(|_| BookLoadError::BookNotFound)?
    }
}

#[derive(Debug)]
pub(crate) enum OperationResult {
    /// Resulting balances per `(account, currency, balance_type)` triple after the accepted operation.
    Accepted(HashMap<(AccountId, Currency, BalanceType), i128>),

    /// AccountId whose book balance caused the rejection.
    Rejected(AccountId),

    /// BookId whose projected balance would exceed the i128 range.
    Overflow(BookId),
}
