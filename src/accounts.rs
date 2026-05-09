use crate::books::{BalanceType, BookId};
use crate::currency::Currency;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{RwLock, mpsc, oneshot};

pub(crate) type AccountId = u64;
pub(crate) type AccountRegistryMap = HashMap<AccountId, Arc<RwLock<AccountState>>>;

#[derive(Deserialize, Serialize, Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum AccountType {
    Debit,
    Credit,
}

#[derive(Debug)]
pub(crate) struct AccountState {
    pub(crate) account_type: AccountType,
    pub(crate) books: HashMap<(Currency, BalanceType), BookId>,
}

pub(crate) struct AccountRegistryActor {
    pub(crate) map: AccountRegistryMap,
    pub(crate) rx: mpsc::Receiver<AccountRegistryCommand>,
}

#[derive(Debug, Error)]
pub(crate) enum AccountLoadError {
    #[error("no such account ID found")]
    AccountNotFound,
    #[error("account registry dropped response channel")]
    Recv(#[from] oneshot::error::RecvError),
}

pub(crate) enum AccountRegistryCommand {
    GetAccounts {
        ids: Vec<AccountId>,
        reply: oneshot::Sender<
            Result<HashMap<AccountId, Arc<RwLock<AccountState>>>, AccountLoadError>,
        >,
    },
    Evict(AccountId),
}

impl AccountRegistryActor {
    pub(crate) async fn account_registry_task(mut self) {
        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                AccountRegistryCommand::GetAccounts { ids, reply } => {
                    let mut result = HashMap::with_capacity(ids.len());

                    //TODO maybe use join! ?
                    for id in ids {
                        let account_arc = self
                            .map
                            .entry(id)
                            .or_insert_with(|| {
                                //TODO asynchronous load from disk
                                Arc::new(RwLock::new(AccountState {
                                    account_type: AccountType::Credit,
                                    books: HashMap::from([
                                        ((123, BalanceType::Available), (id * 3) - 2),
                                        ((123, BalanceType::Current), (id * 3) - 1),
                                        ((123, BalanceType::Hold), id * 3),
                                    ]),
                                }))
                            })
                            .clone();
                        result.insert(id, account_arc);
                    }
                    let _ = reply.send(Ok(result));
                }
                AccountRegistryCommand::Evict(_) => todo!(),
            }
        }
    }
}
