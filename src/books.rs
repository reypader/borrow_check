use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio::task::JoinSet;

use crate::accounts::AccountId;
use crate::journal::JournalEntryBytes;

pub(crate) type BookId = u64;
pub(crate) type BookRegistryMap = HashMap<BookId, Arc<RwLock<BookState>>>;

#[derive(Debug)]
pub(crate) struct BookState {
    pub(crate) running_balance: i128,
    pub(crate) durable_pending_rollup: VecDeque<JournalEntryBytes>,
    pub(crate) pending_journal: VecDeque<i128>,
}

#[derive(Debug, Clone)]
pub(crate) struct InScopeBook {
    pub(crate) account_id: AccountId,
    pub(crate) allow_overdraft: bool,
    pub(crate) state: Arc<RwLock<BookState>>,
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
                    let mut to_load = Vec::new();
                    for id in &ids {
                        if let Some(book_arc) = self.map.get(id) {
                            result.insert(*id, book_arc.clone());
                        } else {
                            to_load.push(*id);
                        }
                    }

                    let mut load_set = JoinSet::new();
                    for id in to_load {
                        load_set.spawn(async move {
                            //TODO asynchronous load from disk
                            let state = Arc::new(RwLock::new(BookState {
                                running_balance: 0,
                                durable_pending_rollup: VecDeque::new(),
                                pending_journal: VecDeque::new(),
                            }));
                            (id, state)
                        });
                    }
                    while let Some(joined) = load_set.join_next().await {
                        let (id, state) = joined.expect("book load task panicked");
                        self.map.insert(id, state.clone());
                        result.insert(id, state);
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
        reply: oneshot::Sender<Result<BookRegistryMap, BookLoadError>>,
    },
    Evict(BookId),
}

#[derive(Deserialize, Serialize, Eq, Hash, PartialEq, Copy, Clone, Debug)]
pub(crate) enum BalanceType {
    Current,
    Available,
    Hold,
}
