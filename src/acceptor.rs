use crate::accounts::{
    AccountId, AccountLoadError, AccountRegistryActor, AccountRegistryCommand, AccountRegistryMap,
    AccountState,
};
use crate::books::{BalanceType, BookRegistryActor, BookRegistryCommand, BookRegistryMap};
use crate::currency::Currency;
use crate::journal::{
    JournalCoverBytes, JournalEntryBytes, JournalHeaderBytes, JournalWriter, WriteCommand,
    WriterBuffer,
};
use crate::operation::{Operation, ValidOperation};
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
        book_registry_tx,
    };
    tokio::spawn(acceptor.acceptor_task(manual_flush_after));
    SystemHandles {
        acceptor_tx,
        account_tx: account_registry_tx,
    }
}

struct Acceptor {
    //TODO make these private and instead create a constructor
    buffer: WriterBuffer,
    writer_tx: mpsc::Sender<WriterBuffer>,
    book_registry_tx: mpsc::Sender<BookRegistryCommand>,
    rx: mpsc::Receiver<AcceptorCommand>,
}

impl Acceptor {
    async fn handle(&mut self, cmd: AcceptorCommand) {
        let AcceptorCommand {
            operations,
            response_tx,
        } = cmd;
        let entries = &operations.entries;
        let mut ids = Vec::with_capacity(entries.len());
        let mut processed_entries = Vec::with_capacity(entries.len());
        let mut totals = HashMap::new();
        for entry in entries {
            // TODO: identify book_id, target_page, target_line
            // TODO: load books using operations.accounts_in_scope book references
            let target_book_id = 0;
            let target_page = 0;
            let target_line = 0;

            ids.push(entry.target_account_id);
            processed_entries.push(JournalEntryBytes {
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

        let (registry_tx, registry_rx) = oneshot::channel();

        let load = async {
            self.book_registry_tx
                .send(BookRegistryCommand::GetBooks {
                    ids,
                    reply: registry_tx,
                })
                .await
                .map_err(|_| OperationError::RegistryFailure)?;
            registry_rx
                .await
                .map_err(|_| OperationError::RegistryFailure)?
                .map_err(|book_load_error| OperationError::FailedToLoadBook) //TODO propagate book_load_error's info
        }
        .await;

        let states = match load {
            Ok(s) => s,
            Err(e) => {
                let _ = response_tx.send(Err(e));
                return;
            }
        };

        //TODO re-validate balances against totals then send OperationResult::Rejected
        let mut ending_balances = HashMap::with_capacity(totals.len());
        for (book_id, book_arc) in states {
            let guard = book_arc.read().await;
            let total_durable =
                guard
                    .durable_pending_rollup
                    .iter()
                    .fold(guard.running_balance, |acc, r| {
                        let val: i128 = r.amount.into();
                        val + acc
                    });
            let merged_balance = guard.pending_journal.iter().fold(total_durable, |acc, r| {
                let val: i128 = r.amount.into();
                val + acc
            });
            // TODO is there a better way to do this? totals, ids, and states should be guaranteed that they contain the same keys.
            let (currency, balance_type, incoming_total) = totals.get(&book_id).unwrap();
            let ending_balance = merged_balance + (*incoming_total);
            if ending_balance < 0 {
                response_tx.send(Ok(OperationResult::Rejected(book_id)));
                return;
            } else {
                //TODO this is a bug, should be account id
                ending_balances.insert(book_id, (*currency, *balance_type, ending_balance));
            }
        }

        let nanos_u128: u128 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before 1970")
            .as_nanos();
        let nanos: u64 = u64::try_from(nanos_u128).expect("timestamp past year 2554");

        let entry_count = u8::try_from(entries.len()).expect("entries exceeded 255"); //safe, we'll be limiting the length at the entry point.
        let record_length = u16::try_from(
            size_of::<JournalHeaderBytes>() + (size_of::<JournalEntryBytes>() * entries.len()),
        )
        .expect("record length exceeded 65535");

        // TODO: identify checksum, prev_hash, operation_id
        let operation_id = 0;
        let checksum = 0;
        let prev_hash = [0; 32];

        let header = JournalHeaderBytes {
            record_length: record_length.into(),
            entry_count: entry_count,
            operation_id: operation_id.into(),
            timestamp_ns: nanos.into(),
            idempotency_key: operations.idempotency_key.into_bytes(),
            checksum: checksum.into(),
            prev_hash,
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
            Err(mpsc::error::SendError(mut failed_commands)) => {
                self.rx.close();
                while let Some(in_flight) = self.rx.recv().await {
                    in_flight.abort()
                }
                for pending in failed_commands.drain(..) {
                    pending.abort(OperationError::AbortedFromFailedFlush)
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
    #[error("failed to load books")]
    FailedToLoadBook,
    #[error("registry task failure")]
    RegistryFailure,
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
            .send(Err(OperationError::AbortedFromFailedFlush));
    }
}

#[derive(Clone)]
pub(crate) struct SystemHandles {
    acceptor_tx: mpsc::Sender<AcceptorCommand>,
    account_tx: mpsc::Sender<AccountRegistryCommand>,
}

impl SystemHandles {
    pub(crate) async fn submit_operation(
        &self,
        op: ValidOperation,
    ) -> Result<OperationResult, OperationError> {
        let (response_sender, response_receiver) = oneshot::channel();
        self.acceptor_tx
            .send(AcceptorCommand {
                operations: op,
                response_tx: response_sender,
            })
            .await
            .map_err(|_| OperationError::AcceptorSystemFailure)?;
        response_receiver.await?
    }

    pub(crate) async fn load_accounts(
        &self,
        ids: Vec<u64>,
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
}

#[derive(Debug)]
pub(crate) enum OperationResult {
    /// Contains the resulting balances after the accepted operation
    Accepted(HashMap<AccountId, (Currency, BalanceType, i128)>),

    /// Contains the AccountId whose balance caused the rejection
    Rejected(AccountId),
}
