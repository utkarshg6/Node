// Copyright (c) 2019, MASQ (https://masq.ai) and/or its affiliates. All rights reserved.

#![cfg(test)]

use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use crate::accountant::payable_dao::{PayableAccount, PayableDao, Payment, TotalInnerEncapsulationPayable};
use crate::accountant::receivable_dao::ReceivableAccount;
use crate::database::dao_utils::{from_time_t, to_time_t};
use crate::test_utils::make_wallet;
use std::time::SystemTime;
use ethereum_types::H256;
use rusqlite::Connection;
use crate::accountant::AccountantError;
use crate::database::connection_wrapper::ConnectionWrapper;
use crate::sub_lib::wallet::Wallet;

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
    total_results: RefCell<Vec<Result<i128,AccountantError>>>
}

impl PayableDao for PayableDaoMock {
    fn more_money_payable(&self, wallet: &Wallet, amount: u128) -> Result<(), AccountantError> {
        self.more_money_payable_parameters
            .lock()
            .unwrap()
            .push((wallet.clone(), amount));
        self.more_money_payable_results.borrow_mut().remove(0)
    }

    fn payment_sent(&self, sent_payment: &Payment) -> Result<(), AccountantError> {
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

    fn total(&self,inner: &dyn TotalInnerEncapsulationPayable) -> Result<i128,AccountantError> {
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

    pub fn total_result(self, result: Result<i128,AccountantError>) -> Self {
        self.total_results.borrow_mut().push(result);
        self
    }
}