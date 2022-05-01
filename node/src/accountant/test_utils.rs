// Copyright (c) 2019, MASQ (https://masq.ai) and/or its affiliates. All rights reserved.

#![cfg(test)]

use crate::accountant::dao_shared_methods::{
    InsertConfiguration, InsertUpdateConfig, InsertUpdateCore, Table, UpdateConfiguration,
};
use crate::accountant::payable_dao::{
    PayableAccount, PayableDao, PayableDaoFactory, Payment, TotalInnerEncapsulationPayable,
};
use crate::accountant::receivable_dao::{ReceivableAccount, ReceivableDao, ReceivableDaoFactory};
use crate::accountant::{Accountant, AccountantError, PaymentCurves};
use crate::banned_dao::{BannedDao, BannedDaoFactory};
use crate::blockchain::blockchain_interface::Transaction;
use crate::bootstrapper::BootstrapperConfig;
use crate::database::connection_wrapper::ConnectionWrapper;
use crate::database::dao_utils::{from_time_t, to_time_t};
use crate::db_config::config_dao::{ConfigDao, ConfigDaoFactory};
use crate::db_config::mocks::ConfigDaoMock;
use crate::sub_lib::wallet::Wallet;
use crate::test_utils::make_wallet;
use crate::test_utils::persistent_configuration_mock::PersistentConfigurationMock;
use ethereum_types::H256;
use itertools::Either;
use rusqlite::{Error, Transaction as RusqliteTransaction};
use std::cell::RefCell;
use std::ptr::addr_of;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

pub fn make_receivable_account(n: u64, expected_delinquent: bool) -> ReceivableAccount {
    let now = to_time_t(SystemTime::now());
    ReceivableAccount {
        wallet: make_wallet(&format!(
            "wallet{}{}",
            n,
            if expected_delinquent { "d" } else { "n" }
        )),
        balance: (n * 1_000_000_000) as i128,
        last_received_timestamp: from_time_t(now as i64 - (n as i64)),
    }
}

pub fn make_payable_account(n: u64) -> PayableAccount {
    let now = to_time_t(SystemTime::now());
    PayableAccount {
        wallet: make_wallet(&format!("wallet{}", n)),
        balance: (n * 1_000_000_000) as i128,
        last_paid_timestamp: from_time_t(now as i64 - (n as i64)),
        pending_payment_transaction: None,
    }
}

#[derive(Debug, Default)]
pub struct PayableDaoMock {
    account_status_parameters: Arc<Mutex<Vec<Wallet>>>,
    account_status_results: RefCell<Vec<Option<PayableAccount>>>,
    more_money_payable_parameters: Arc<Mutex<Vec<(Wallet, u128)>>>,
    more_money_payable_results: RefCell<Vec<Result<(), AccountantError>>>,
    non_pending_payables_results: RefCell<Vec<Vec<PayableAccount>>>,
    payment_sent_parameters: Arc<Mutex<Vec<Payment>>>,
    payment_sent_results: RefCell<Vec<Result<(), AccountantError>>>,
    top_records_parameters: Arc<Mutex<Vec<(i128, u64)>>>,
    top_records_results: RefCell<Vec<Result<Vec<PayableAccount>, AccountantError>>>,
    total_results: RefCell<Vec<Result<i128, AccountantError>>>,
}

impl PayableDao for PayableDaoMock {
    fn more_money_payable(
        &self,
        wallet: &Wallet,
        amount: u128,
        insert_update_core: &dyn InsertUpdateCore,
    ) -> Result<(), AccountantError> {
        self.more_money_payable_parameters
            .lock()
            .unwrap()
            .push((wallet.clone(), amount));
        self.more_money_payable_results.borrow_mut().remove(0)
    }

    fn payment_sent(
        &self,
        sent_payment: &Payment,
        insert_update_core: &dyn InsertUpdateCore,
    ) -> Result<(), AccountantError> {
        self.payment_sent_parameters
            .lock()
            .unwrap()
            .push(sent_payment.clone());
        self.payment_sent_results.borrow_mut().remove(0)
    }

    fn payment_confirmed(
        &self,
        _wallet: &Wallet,
        _amount: u128,
        _confirmation_noticed_timestamp: SystemTime,
        _transaction_hash: H256,
    ) -> Result<(), AccountantError> {
        unimplemented!("SC-925: TODO")
    }

    fn account_status(&self, wallet: &Wallet) -> Option<PayableAccount> {
        self.account_status_parameters
            .lock()
            .unwrap()
            .push(wallet.clone());
        self.account_status_results.borrow_mut().remove(0)
    }

    fn non_pending_payables(&self) -> Vec<PayableAccount> {
        if self.non_pending_payables_results.borrow().is_empty() {
            vec![]
        } else {
            self.non_pending_payables_results.borrow_mut().remove(0)
        }
    }

    fn top_records(
        &self,
        minimum_amount: i128,
        maximum_age: u64,
    ) -> Result<Vec<PayableAccount>, AccountantError> {
        self.top_records_parameters
            .lock()
            .unwrap()
            .push((minimum_amount, maximum_age));
        self.top_records_results.borrow_mut().remove(0)
    }

    fn total(&self, inner: &dyn TotalInnerEncapsulationPayable) -> Result<i128, AccountantError> {
        self.total_results.borrow_mut().remove(0)
    }
}

impl PayableDaoMock {
    pub fn new() -> PayableDaoMock {
        PayableDaoMock::default()
    }

    pub fn more_money_payable_parameters(
        mut self,
        parameters: Arc<Mutex<Vec<(Wallet, u128)>>>,
    ) -> Self {
        self.more_money_payable_parameters = parameters;
        self
    }

    pub fn more_money_payable_result(self, result: Result<(), AccountantError>) -> Self {
        self.more_money_payable_results.borrow_mut().push(result);
        self
    }

    pub fn non_pending_payables_result(self, result: Vec<PayableAccount>) -> Self {
        self.non_pending_payables_results.borrow_mut().push(result);
        self
    }

    pub fn payment_sent_parameters(mut self, parameters: Arc<Mutex<Vec<Payment>>>) -> Self {
        self.payment_sent_parameters = parameters;
        self
    }

    pub fn payment_sent_result(self, result: Result<(), AccountantError>) -> Self {
        self.payment_sent_results.borrow_mut().push(result);
        self
    }

    pub fn top_records_parameters(mut self, parameters: &Arc<Mutex<Vec<(i128, u64)>>>) -> Self {
        self.top_records_parameters = parameters.clone();
        self
    }

    pub fn top_records_result(self, result: Result<Vec<PayableAccount>, AccountantError>) -> Self {
        self.top_records_results.borrow_mut().push(result);
        self
    }

    pub fn total_result(self, result: Result<i128, AccountantError>) -> Self {
        self.total_results.borrow_mut().push(result);
        self
    }
}

#[derive(Default)]
pub struct InsertUpdateCoreMock {
    update_params: Arc<Mutex<Vec<(String, String, String, Vec<String>)>>>, //trait-object-like params tested specially
    update_results: RefCell<Vec<Result<(), String>>>,
    insert_or_update_params: Arc<Mutex<Vec<(String, i128, (String, String, Table, Vec<String>))>>>, //I have to skip the sql params which cannot be handled in a test economically
    insert_or_update_results: RefCell<Vec<Result<(), String>>>,
    connection_wrapper_as_pointer_to_compare: Option<*const dyn ConnectionWrapper>,
}

impl InsertUpdateCore for InsertUpdateCoreMock {
    fn insert(
        &self,
        conn: &dyn ConnectionWrapper,
        config: &dyn InsertConfiguration,
    ) -> Result<(), Error> {
        todo!()
    }

    fn update<'a>(
        &self,
        conn: Either<&dyn ConnectionWrapper, &RusqliteTransaction>,
        config: &'a (dyn UpdateConfiguration<'a> + 'a),
    ) -> Result<(), String> {
        let owned_params: Vec<String> = config
            .update_params()
            .all_rusqlite_params()
            .into_iter()
            .map(|(str, _to_sql)| str.to_string())
            .collect();
        self.update_params.lock().unwrap().push((
            config.select_sql(),
            config.update_sql().to_string(),
            config.table(),
            owned_params,
        ));
        if let Some(conn_wrapp_pointer) = self.connection_wrapper_as_pointer_to_compare {
            assert_eq!(conn_wrapp_pointer, addr_of!(*conn.left().unwrap()))
        }
        self.update_results.borrow_mut().remove(0)
    }

    fn upsert(
        &self,
        conn: &dyn ConnectionWrapper,
        wallet: &str,
        amount: i128,
        config: InsertUpdateConfig,
    ) -> Result<(), String> {
        let owned_params: Vec<String> = config
            .params
            .all_rusqlite_params()
            .into_iter()
            .map(|(str, _to_sql)| str.to_string())
            .collect();
        self.insert_or_update_params.lock().unwrap().push((
            wallet.to_string(),
            amount,
            (
                config.update_sql.to_string(),
                config.insert_sql.to_string(),
                config.table,
                owned_params,
            ),
        ));
        if let Some(conn_wrapp_pointer) = self.connection_wrapper_as_pointer_to_compare {
            assert_eq!(conn_wrapp_pointer, addr_of!(*conn))
        }
        self.insert_or_update_results.borrow_mut().remove(0)
    }
}

impl InsertUpdateCoreMock {
    pub fn update_params(
        mut self,
        params: &Arc<Mutex<Vec<(String, String, String, Vec<String>)>>>,
    ) -> Self {
        self.update_params = params.clone();
        self
    }

    pub fn update_result(self, result: Result<(), String>) -> Self {
        self.update_results.borrow_mut().push(result);
        self
    }

    pub fn insert_or_update_params(
        mut self,
        params: &Arc<Mutex<Vec<(String, i128, (String, String, Table, Vec<String>))>>>,
    ) -> Self {
        self.insert_or_update_params = params.clone();
        self
    }

    pub fn insert_or_update_results(self, result: Result<(), String>) -> Self {
        self.insert_or_update_results.borrow_mut().push(result);
        self
    }
}

pub struct PayableDaoFactoryMock {
    called: Rc<RefCell<bool>>,
    mock: RefCell<Option<PayableDaoMock>>,
}

impl PayableDaoFactory for PayableDaoFactoryMock {
    fn make(&self) -> Box<dyn PayableDao> {
        *self.called.borrow_mut() = true;
        Box::new(self.mock.borrow_mut().take().unwrap())
    }
}

impl PayableDaoFactoryMock {
    pub fn new(mock: PayableDaoMock) -> Self {
        Self {
            called: Rc::new(RefCell::new(false)),
            mock: RefCell::new(Some(mock)),
        }
    }

    pub fn called(mut self, called: &Rc<RefCell<bool>>) -> Self {
        self.called = called.clone();
        self
    }
}

#[derive(Debug, Default)]
pub struct ReceivableDaoMock {
    account_status_parameters: Arc<Mutex<Vec<Wallet>>>,
    account_status_results: RefCell<Vec<Option<ReceivableAccount>>>,
    more_money_receivable_parameters: Arc<Mutex<Vec<(Wallet, u128)>>>,
    more_money_receivable_results: RefCell<Vec<Result<(), AccountantError>>>,
    more_money_received_parameters: Arc<Mutex<Vec<Vec<Transaction>>>>,
    more_money_received_results: RefCell<Vec<Result<(), AccountantError>>>,
    receivables_results: RefCell<Vec<Vec<ReceivableAccount>>>,
    new_delinquencies_parameters: Arc<Mutex<Vec<(SystemTime, PaymentCurves)>>>,
    new_delinquencies_results: RefCell<Vec<Result<Vec<ReceivableAccount>, AccountantError>>>,
    paid_delinquencies_parameters: Arc<Mutex<Vec<PaymentCurves>>>,
    paid_delinquencies_results: RefCell<Vec<Result<Vec<ReceivableAccount>, AccountantError>>>,
    top_records_parameters: Arc<Mutex<Vec<(i128, u64)>>>,
    top_records_results: RefCell<Vec<Result<Vec<ReceivableAccount>, AccountantError>>>,
    total_results: RefCell<Vec<i128>>,
}

impl ReceivableDao for ReceivableDaoMock {
    fn more_money_receivable(
        &self,
        wallet: &Wallet,
        amount: u128,
        insert_update_core: &dyn InsertUpdateCore,
    ) -> Result<(), AccountantError> {
        self.more_money_receivable_parameters
            .lock()
            .unwrap()
            .push((wallet.clone(), amount));
        self.more_money_receivable_results.borrow_mut().remove(0)
    }

    fn more_money_received(
        &mut self,
        transactions: &[Transaction],
        insert_update_code: &dyn InsertUpdateCore,
    ) -> Result<(), AccountantError> {
        self.more_money_received_parameters
            .lock()
            .unwrap()
            .push(transactions.to_vec());
        self.more_money_received_results.borrow_mut().remove(0)
    }

    fn single_account_status(&self, wallet: &Wallet) -> Option<ReceivableAccount> {
        self.account_status_parameters
            .lock()
            .unwrap()
            .push(wallet.clone());

        self.account_status_results.borrow_mut().remove(0)
    }

    fn all_receivable_accounts(&self) -> Vec<ReceivableAccount> {
        self.receivables_results.borrow_mut().remove(0)
    }

    fn new_delinquencies(
        &self,
        now: SystemTime,
        payment_curves: &PaymentCurves,
    ) -> Result<Vec<ReceivableAccount>, AccountantError> {
        self.new_delinquencies_parameters
            .lock()
            .unwrap()
            .push((now, payment_curves.clone()));
        if self.new_delinquencies_results.borrow().is_empty() {
            Ok(vec![])
        } else {
            self.new_delinquencies_results.borrow_mut().remove(0)
        }
    }

    fn paid_delinquencies(
        &self,
        payment_curves: &PaymentCurves,
    ) -> Result<Vec<ReceivableAccount>, AccountantError> {
        self.paid_delinquencies_parameters
            .lock()
            .unwrap()
            .push(payment_curves.clone());
        if self.paid_delinquencies_results.borrow().is_empty() {
            Ok(vec![])
        } else {
            self.paid_delinquencies_results.borrow_mut().remove(0)
        }
    }

    fn top_records(
        &self,
        minimum_amount: i128,
        maximum_age: u64,
    ) -> Result<Vec<ReceivableAccount>, AccountantError> {
        self.top_records_parameters
            .lock()
            .unwrap()
            .push((minimum_amount, maximum_age));
        self.top_records_results.borrow_mut().remove(0)
    }

    fn total(&self) -> i128 {
        self.total_results.borrow_mut().remove(0)
    }
}

impl ReceivableDaoMock {
    pub fn new() -> ReceivableDaoMock {
        Self::default()
    }

    pub fn more_money_receivable_parameters(
        mut self,
        parameters: &Arc<Mutex<Vec<(Wallet, u128)>>>,
    ) -> Self {
        self.more_money_receivable_parameters = parameters.clone();
        self
    }

    pub fn more_money_receivable_result(self, result: Result<(), AccountantError>) -> Self {
        self.more_money_receivable_results.borrow_mut().push(result);
        self
    }

    pub fn more_money_received_parameters(
        mut self,
        parameters: &Arc<Mutex<Vec<Vec<Transaction>>>>,
    ) -> Self {
        self.more_money_received_parameters = parameters.clone();
        self
    }

    pub fn more_money_received_result(self, result: Result<(), AccountantError>) -> Self {
        self.more_money_received_results.borrow_mut().push(result);
        self
    }

    pub fn new_delinquencies_parameters(
        mut self,
        parameters: &Arc<Mutex<Vec<(SystemTime, PaymentCurves)>>>,
    ) -> Self {
        self.new_delinquencies_parameters = parameters.clone();
        self
    }

    pub fn new_delinquencies_result(
        self,
        result: Result<Vec<ReceivableAccount>, AccountantError>,
    ) -> ReceivableDaoMock {
        self.new_delinquencies_results.borrow_mut().push(result);
        self
    }

    pub fn paid_delinquencies_parameters(
        mut self,
        parameters: &Arc<Mutex<Vec<PaymentCurves>>>,
    ) -> Self {
        self.paid_delinquencies_parameters = parameters.clone();
        self
    }

    pub fn paid_delinquencies_result(
        self,
        result: Result<Vec<ReceivableAccount>, AccountantError>,
    ) -> ReceivableDaoMock {
        self.paid_delinquencies_results.borrow_mut().push(result);
        self
    }

    pub fn top_records_parameters(mut self, parameters: &Arc<Mutex<Vec<(i128, u64)>>>) -> Self {
        self.top_records_parameters = parameters.clone();
        self
    }

    pub fn top_records_result(
        self,
        result: Result<Vec<ReceivableAccount>, AccountantError>,
    ) -> Self {
        self.top_records_results.borrow_mut().push(result);
        self
    }

    pub fn total_result(self, result: i128) -> Self {
        self.total_results.borrow_mut().push(result);
        self
    }
}

pub struct ReceivableDaoFactoryMock {
    called: Rc<RefCell<bool>>,
    mock: RefCell<Option<ReceivableDaoMock>>,
}

impl ReceivableDaoFactory for ReceivableDaoFactoryMock {
    fn make(&self) -> Box<dyn ReceivableDao> {
        *self.called.borrow_mut() = true;
        Box::new(self.mock.borrow_mut().take().unwrap())
    }
}

impl ReceivableDaoFactoryMock {
    pub fn new(mock: ReceivableDaoMock) -> Self {
        Self {
            called: Rc::new(RefCell::new(false)),
            mock: RefCell::new(Some(mock)),
        }
    }

    pub fn called(mut self, called: &Rc<RefCell<bool>>) -> Self {
        self.called = called.clone();
        self
    }
}

pub struct BannedDaoFactoryMock {
    called: Rc<RefCell<bool>>,
    mock: RefCell<Option<BannedDaoMock>>,
}

impl BannedDaoFactory for BannedDaoFactoryMock {
    fn make(&self) -> Box<dyn BannedDao> {
        *self.called.borrow_mut() = true;
        Box::new(self.mock.borrow_mut().take().unwrap())
    }
}

impl BannedDaoFactoryMock {
    pub fn new(mock: BannedDaoMock) -> Self {
        Self {
            called: Rc::new(RefCell::new(false)),
            mock: RefCell::new(Some(mock)),
        }
    }

    pub fn called(mut self, called: &Rc<RefCell<bool>>) -> Self {
        self.called = called.clone();
        self
    }
}

pub struct ConfigDaoFactoryMock {
    called: Rc<RefCell<bool>>,
    mock: RefCell<Option<ConfigDaoMock>>,
}

impl ConfigDaoFactory for ConfigDaoFactoryMock {
    fn make(&self) -> Box<dyn ConfigDao> {
        *self.called.borrow_mut() = true;
        Box::new(self.mock.borrow_mut().take().unwrap())
    }
}

impl ConfigDaoFactoryMock {
    pub fn new(mock: ConfigDaoMock) -> Self {
        Self {
            called: Rc::new(RefCell::new(false)),
            mock: RefCell::new(Some(mock)),
        }
    }

    pub fn called(mut self, called: &Rc<RefCell<bool>>) -> Self {
        self.called = called.clone();
        self
    }
}

#[derive(Debug, Default)]
pub struct BannedDaoMock {
    ban_list_parameters: Arc<Mutex<Vec<()>>>,
    ban_list_results: RefCell<Vec<Vec<Wallet>>>,
    ban_parameters: Arc<Mutex<Vec<Wallet>>>,
    unban_parameters: Arc<Mutex<Vec<Wallet>>>,
}

impl BannedDao for BannedDaoMock {
    fn ban_list(&self) -> Vec<Wallet> {
        self.ban_list_parameters.lock().unwrap().push(());
        self.ban_list_results.borrow_mut().remove(0)
    }

    fn ban(&self, wallet: &Wallet) {
        self.ban_parameters.lock().unwrap().push(wallet.clone());
    }

    fn unban(&self, wallet: &Wallet) {
        self.unban_parameters.lock().unwrap().push(wallet.clone());
    }
}

impl BannedDaoMock {
    pub fn new() -> Self {
        Self {
            ban_list_parameters: Arc::new(Mutex::new(vec![])),
            ban_list_results: RefCell::new(vec![]),
            ban_parameters: Arc::new(Mutex::new(vec![])),
            unban_parameters: Arc::new(Mutex::new(vec![])),
        }
    }

    pub fn ban_list_result(self, result: Vec<Wallet>) -> Self {
        self.ban_list_results.borrow_mut().push(result);
        self
    }

    pub fn ban_parameters(mut self, parameters: &Arc<Mutex<Vec<Wallet>>>) -> Self {
        self.ban_parameters = parameters.clone();
        self
    }

    pub fn unban_parameters(mut self, parameters: &Arc<Mutex<Vec<Wallet>>>) -> Self {
        self.unban_parameters = parameters.clone();
        self
    }
}

pub fn make_accountant(
    config_opt: Option<BootstrapperConfig>,
    payable_dao_opt: Option<PayableDaoMock>,
    receivable_dao_opt: Option<ReceivableDaoMock>,
    banned_dao_opt: Option<BannedDaoMock>,
    persistent_config_opt: Option<PersistentConfigurationMock>,
) -> Accountant {
    let payable_dao_factory =
        PayableDaoFactoryMock::new(payable_dao_opt.unwrap_or(PayableDaoMock::new()));
    let receivable_dao_factory =
        ReceivableDaoFactoryMock::new(receivable_dao_opt.unwrap_or(ReceivableDaoMock::new()));
    let banned_dao_factory =
        BannedDaoFactoryMock::new(banned_dao_opt.unwrap_or(BannedDaoMock::new()));
    let mut subject = Accountant::new(
        &config_opt.unwrap_or(BootstrapperConfig::new()),
        Box::new(payable_dao_factory),
        Box::new(receivable_dao_factory),
        Box::new(banned_dao_factory),
        Box::new(ConfigDaoFactoryMock::new(ConfigDaoMock::new())),
    );
    subject.persistent_configuration = if let Some(persistent_config) = persistent_config_opt {
        Box::new(persistent_config)
    } else {
        Box::new(PersistentConfigurationMock::new())
    };
    subject
}
