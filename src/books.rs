use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{RwLock, mpsc, oneshot};

use crate::journal::JournalEntryBytes;

pub(crate) type BookId = u64;
pub(crate) type BookRegistryMap = HashMap<BookId, Arc<RwLock<BookState>>>;

pub(crate) struct BookState {
    pub(crate) running_balance: i128,
    pub(crate) durable_pending_rollup: Vec<JournalEntryBytes>,
    pub(crate) pending_journal: Vec<JournalEntryBytes>,
}

pub(crate) struct BookRegistryActor {
    pub(crate) map: BookRegistryMap,
    pub(crate) rx: mpsc::Receiver<BookRegistryCommand>,
}

impl BookRegistryActor {
    pub(crate) async fn book_registry_task(mut self) {
        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                BookRegistryCommand::GetBooks { ids, reply } => {
                    let mut result = HashMap::with_capacity(ids.len());

                    //TODO maybe use join! ?
                    for id in ids {
                        let book_arc = self
                            .map
                            .entry(id)
                            .or_insert_with(|| {
                                //TODO asynchronous load from disk
                                Arc::new(RwLock::new(BookState {
                                    running_balance: 0,
                                    durable_pending_rollup: Vec::new(),
                                    pending_journal: Vec::new(),
                                }))
                            })
                            .clone();
                        result.insert(id, book_arc);
                    }
                    let _ = reply.send(Ok(result));
                }
                BookRegistryCommand::Evict(_) => todo!(),
            }
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum BookLoadError {
    #[error("no such book ID found")]
    BookNotFound,
}

pub(crate) enum BookRegistryCommand {
    GetBooks {
        ids: Vec<BookId>,
        reply: oneshot::Sender<Result<HashMap<BookId, Arc<RwLock<BookState>>>, BookLoadError>>,
    },
    Evict(BookId),
}

#[derive(Deserialize, Serialize, Eq, Hash, PartialEq, Copy, Clone, Debug)]
pub(crate) enum BalanceType {
    Current,
    Available,
    Hold,
}
