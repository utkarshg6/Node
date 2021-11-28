// Copyright (c) 2019, MASQ (https://masq.ai) and/or its affiliates. All rights reserved.

pub mod dao_shared_methods;
pub mod payable_dao;
pub mod receivable_dao;

#[cfg(test)]
pub mod test_utils;

use crate::accountant::dao_shared_methods::InsertUpdateCoreReal;
use crate::accountant::payable_dao::{
    PayableAccount, PayableDaoFactory, Payment, TotalInnerEncapsulationPayableReal,
};
use crate::accountant::receivable_dao::{
    ReceivableAccount, ReceivableDaoFactory, ReceivableDaoReal, ReceivableError,
};
use crate::banned_dao::{BannedDao, BannedDaoFactory};
use crate::blockchain::blockchain_bridge::RetrieveTransactions;
use crate::blockchain::blockchain_interface::{BlockchainError, Transaction};
use crate::bootstrapper::BootstrapperConfig;
use crate::db_config::config_dao::ConfigDaoFactory;
use crate::db_config::persistent_configuration::{
    PersistentConfiguration, PersistentConfigurationReal,
};
use crate::sub_lib::accountant::AccountantConfig;
use crate::sub_lib::accountant::AccountantSubs;
use crate::sub_lib::accountant::ReportExitServiceConsumedMessage;
use crate::sub_lib::accountant::ReportExitServiceProvidedMessage;
use crate::sub_lib::accountant::ReportRoutingServiceConsumedMessage;
use crate::sub_lib::accountant::ReportRoutingServiceProvidedMessage;
use crate::sub_lib::blockchain_bridge::ReportAccountsPayable;
use crate::sub_lib::logger::Logger;
use crate::sub_lib::peer_actors::{BindMessage, StartMessage};
use crate::sub_lib::utils::NODE_MAILBOX_CAPACITY;
use crate::sub_lib::wallet::Wallet;
use actix::Actor;
use actix::Addr;
use actix::AsyncContext;
use actix::Context;
use actix::Handler;
use actix::Message;
use actix::Recipient;
use futures::future::Future;
use itertools::Itertools;
use lazy_static::lazy_static;
use masq_lib::messages::UiMessageError::UnexpectedMessage;
use masq_lib::messages::{FromMessageBody, ToMessageBody, UiFinancialsRequest, UiMessageError};
use masq_lib::messages::{UiFinancialsResponse, UiPayableAccount, UiReceivableAccount};
use masq_lib::ui_gateway::MessageTarget::ClientId;
use masq_lib::ui_gateway::{NodeFromUiMessage, NodeToUiMessage};
use payable_dao::PayableDao;
use receivable_dao::ReceivableDao;
use std::fmt::{Display, Formatter};
use std::ops::Add;
use std::thread;
use std::time::{Duration, SystemTime};

pub const WEI_IN_GWEI: i128 = 1_000_000_000;

pub const CRASH_KEY: &str = "ACCOUNTANT";
pub const DEFAULT_PAYABLE_SCAN_INTERVAL: u64 = 3600; // one hour
pub const DEFAULT_PAYMENT_RECEIVED_SCAN_INTERVAL: u64 = 3600; // one hour

const SECONDS_PER_DAY: u64 = 86_400;

lazy_static! {
    pub static ref PAYMENT_CURVES: PaymentCurves = PaymentCurves {
        payment_suggested_after_sec: SECONDS_PER_DAY,
        payment_grace_before_ban_sec: SECONDS_PER_DAY,
        permanent_debt_allowed_wei: 10_000_000, //TODO value that used to be gwei
        balance_to_decrease_from_wei: 1_000_000_000, //TODO value that used to be gwei
        balance_decreases_for_sec: 30 * SECONDS_PER_DAY, //TODO value that used to be gwei
        unban_when_balance_below_wei: 10_000_000, //TODO value that used to be gwei
    };
}

#[derive(PartialEq, Debug)]
pub enum AccountantError {
    PayableError(PayableError),
    ReceivableError(ReceivableError),
    //PaymentCurvesError
    RecordServiceDataOverflow(SignConversionError),
}

impl Display for AccountantError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AccountantError::PayableError(msg) => write!(f, "Error from payable: {:?}", msg),
            AccountantError::ReceivableError(msg) => write!(f, "Error from receivable: {:?}", msg),
            _ => unimplemented!(),
        }
    }
}

impl AccountantError {
    pub fn extend(self, msg_extension: &str) -> Self {
        match self {
            //TODO maybe write a test that both sides are always of the same kind
            AccountantError::ReceivableError(ReceivableError::RusqliteError(msg)) => {
                AccountantError::ReceivableError(ReceivableError::RusqliteError(Self::add_str(
                    msg,
                    msg_extension,
                )))
            }
            AccountantError::ReceivableError(ReceivableError::ConfigurationError(msg)) => {
                unimplemented!()
            }
            AccountantError::ReceivableError(ReceivableError::Overflow(sign_conv_err)) => {
                match sign_conv_err {
                    SignConversionError::U128(msg) => {
                        SignConversionError::U128(Self::add_str(msg, msg_extension))
                            .into_receivable()
                    }
                    SignConversionError::U64(msg) => unimplemented!(),
                    SignConversionError::I128(msg) => unimplemented!(),
                }
            }
            AccountantError::PayableError(PayableError::RusqliteError(msg)) => {
                AccountantError::PayableError(PayableError::RusqliteError(Self::add_str(
                    msg,
                    msg_extension,
                )))
            }
            AccountantError::PayableError(PayableError::Overflow(sign_conv_err)) => {
                match sign_conv_err {
                    SignConversionError::U128(msg) => {
                        SignConversionError::U128(Self::add_str(msg, msg_extension)).into_payable()
                    }
                    SignConversionError::U64(msg) => unimplemented!(),
                    SignConversionError::I128(msg) => unimplemented!(),
                }
            }
            x => x,
        }
    }

    fn add_str(msg: String, msg_extension: &str) -> String {
        format!("{}; {}", msg, msg_extension)
    }
}

#[derive(PartialEq, Debug)]
pub enum PayableError {
    Overflow(SignConversionError),
    RusqliteError(String),
}

#[derive(PartialEq, Debug)]
pub enum SignConversionError {
    U64(String),
    U128(String),
    I128(String),
}

impl SignConversionError {
    fn into_payable(self) -> AccountantError {
        AccountantError::PayableError(PayableError::Overflow(self))
    }
    fn into_receivable(self) -> AccountantError {
        AccountantError::ReceivableError(ReceivableError::Overflow(self))
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct PaymentCurves {
    pub payment_suggested_after_sec: u64,
    pub payment_grace_before_ban_sec: u64,
    pub permanent_debt_allowed_wei: u128,
    pub balance_to_decrease_from_wei: u128,
    pub balance_decreases_for_sec: u64,
    pub unban_when_balance_below_wei: u128,
}

impl PaymentCurves {
    pub fn sugg_and_grace(&self, now: i64) -> i64 {
        now - u64_to_signed(self.payment_suggested_after_sec).expect("we allowed an overflow")
            - u64_to_signed(self.payment_grace_before_ban_sec).expect("we allowed an overflow")
    }

    pub fn sugg_thru_decreasing(&self, now: i64) -> i64 {
        self.sugg_and_grace(now)
            - u64_to_signed(self.balance_decreases_for_sec).expect("we allowed an overflow")
    }
}

pub fn to_wei_in_i128(gwei: u64) -> i128 {
    i128::from(gwei) * WEI_IN_GWEI
}

pub struct Accountant {
    config: AccountantConfig,
    consuming_wallet: Option<Wallet>,
    earning_wallet: Wallet,
    payable_dao: Box<dyn PayableDao>,
    receivable_dao: Box<dyn ReceivableDao>,
    banned_dao: Box<dyn BannedDao>,
    persistent_configuration: Box<dyn PersistentConfiguration>,
    report_accounts_payable_sub: Option<Recipient<ReportAccountsPayable>>,
    retrieve_transactions_sub: Option<Recipient<RetrieveTransactions>>,
    report_new_payments_sub: Option<Recipient<ReceivedPayments>>,
    report_sent_payments_sub: Option<Recipient<SentPayments>>,
    ui_message_sub: Option<Recipient<NodeToUiMessage>>,
    logger: Logger,
}

impl Actor for Accountant {
    type Context = Context<Self>;
}

#[derive(Debug, Eq, Message, PartialEq)]
pub struct ReceivedPayments {
    payments: Vec<Transaction>,
}

#[derive(Debug, Eq, Message, PartialEq)]
pub struct SentPayments {
    pub payments: Vec<Result<Payment, BlockchainError>>,
}

impl Handler<BindMessage> for Accountant {
    type Result = ();

    fn handle(&mut self, msg: BindMessage, ctx: &mut Self::Context) -> Self::Result {
        self.handle_bind_message(msg);
        ctx.set_mailbox_capacity(NODE_MAILBOX_CAPACITY);
    }
}

impl Handler<StartMessage> for Accountant {
    type Result = ();

    fn handle(&mut self, _msg: StartMessage, ctx: &mut Self::Context) -> Self::Result {
        self.handle_start_message();

        ctx.run_interval(self.config.payable_scan_interval, |accountant, _ctx| {
            accountant.scan_for_payables();
        });

        ctx.run_interval(
            self.config.payment_received_scan_interval,
            |accountant, _ctx| {
                accountant.scan_for_received_payments();
                accountant.scan_for_delinquencies();
            },
        );
    }
}

impl Handler<ReceivedPayments> for Accountant {
    type Result = ();

    fn handle(&mut self, msg: ReceivedPayments, _ctx: &mut Self::Context) -> Self::Result {
        self.handle_received_payments(msg);
    }
}

impl Handler<SentPayments> for Accountant {
    type Result = ();

    fn handle(&mut self, sent_payments: SentPayments, _ctx: &mut Self::Context) -> Self::Result {
        self.handle_sent_payments(sent_payments);
    }
}

impl Handler<ReportRoutingServiceProvidedMessage> for Accountant {
    type Result = ();

    fn handle(
        &mut self,
        msg: ReportRoutingServiceProvidedMessage,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        self.handle_report_routing_service_provided_message(msg);
    }
}

impl Handler<ReportExitServiceProvidedMessage> for Accountant {
    type Result = ();

    fn handle(
        &mut self,
        msg: ReportExitServiceProvidedMessage,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        self.handle_report_exit_service_provided_message(msg);
    }
}

impl Handler<ReportRoutingServiceConsumedMessage> for Accountant {
    type Result = ();

    fn handle(
        &mut self,
        msg: ReportRoutingServiceConsumedMessage,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        self.handle_report_routing_service_consumed_message(msg);
    }
}

impl Handler<ReportExitServiceConsumedMessage> for Accountant {
    type Result = ();

    fn handle(
        &mut self,
        msg: ReportExitServiceConsumedMessage,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        self.handle_report_exit_service_consumed_message(msg);
    }
}

impl Handler<NodeFromUiMessage> for Accountant {
    type Result = ();

    fn handle(&mut self, msg: NodeFromUiMessage, _ctx: &mut Self::Context) -> Self::Result {
        self.handle_node_from_ui_message(msg);
    }
}

impl Accountant {
    pub fn new(
        config: &BootstrapperConfig,
        payable_dao_factory: Box<dyn PayableDaoFactory>,
        receivable_dao_factory: Box<dyn ReceivableDaoFactory>,
        banned_dao_factory: Box<dyn BannedDaoFactory>,
        config_dao_factory: Box<dyn ConfigDaoFactory>,
    ) -> Accountant {
        Accountant {
            config: config.accountant_config.clone(),
            consuming_wallet: config.consuming_wallet.clone(),
            earning_wallet: config.earning_wallet.clone(),
            payable_dao: payable_dao_factory.make(),
            receivable_dao: receivable_dao_factory.make(),
            banned_dao: banned_dao_factory.make(),
            persistent_configuration: Box::new(PersistentConfigurationReal::new(
                config_dao_factory.make(),
            )),
            report_accounts_payable_sub: None,
            retrieve_transactions_sub: None,
            report_new_payments_sub: None,
            report_sent_payments_sub: None,
            ui_message_sub: None,
            logger: Logger::new("Accountant"),
        }
    }

    pub fn make_subs_from(addr: &Addr<Accountant>) -> AccountantSubs {
        AccountantSubs {
            bind: addr.clone().recipient::<BindMessage>(),
            start: addr.clone().recipient::<StartMessage>(),
            report_routing_service_provided: addr
                .clone()
                .recipient::<ReportRoutingServiceProvidedMessage>(),
            report_exit_service_provided: addr
                .clone()
                .recipient::<ReportExitServiceProvidedMessage>(),
            report_routing_service_consumed: addr
                .clone()
                .recipient::<ReportRoutingServiceConsumedMessage>(),
            report_exit_service_consumed: addr
                .clone()
                .recipient::<ReportExitServiceConsumedMessage>(),
            report_new_payments: addr.clone().recipient::<ReceivedPayments>(),
            report_sent_payments: addr.clone().recipient::<SentPayments>(),
            ui_message_sub: addr.clone().recipient::<NodeFromUiMessage>(),
        }
    }

    fn scan_for_payables(&mut self) {
        debug!(self.logger, "Scanning for payables");
        let future_logger = self.logger.clone();

        let all_non_pending_payables = self.payable_dao.non_pending_payables();
        debug!(
            self.logger,
            "{}",
            Self::investigate_debt_extremes(&all_non_pending_payables)
        );
        let qualified_payables = all_non_pending_payables
            .into_iter()
            .filter(Accountant::should_pay)
            .collect::<Vec<PayableAccount>>();
        info!(
            self.logger,
            "Chose {} qualified debts to pay",
            qualified_payables.len()
        );
        debug!(
            self.logger,
            "{}",
            Self::payments_debug_summary(&qualified_payables)
        );
        if !qualified_payables.is_empty() {
            let report_sent_payments = self.report_sent_payments_sub.clone();
            // TODO: This is bad code. The ReportAccountsPayable message should have no result;
            // instead, when the BlockchainBridge completes processing the ReportAccountsPayable
            // message, it should send a SentPayments message back to the Accountant. There should
            // be no future here: mixing futures and Actors is a bad idea.
            let future = self
                .report_accounts_payable_sub
                .as_ref()
                .expect("BlockchainBridge is unbound")
                .send(ReportAccountsPayable {
                    accounts: qualified_payables,
                })
                .then(move |results| match results {
                    Ok(Ok(results)) => {
                        report_sent_payments
                            .expect("Accountant is unbound")
                            .try_send(SentPayments { payments: results })
                            .expect("Accountant is dead");
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        warning!(future_logger, "{}", e);
                        Ok(())
                    }
                    Err(e) => {
                        error!(
                            future_logger,
                            "Unable to send ReportAccountsPayable: {:?}", e
                        );
                        thread::sleep(Duration::from_secs(1));
                        panic!("Unable to send ReportAccountsPayable: {:?}", e);
                    }
                });
            actix::spawn(future);
        }
    }

    fn scan_for_delinquencies(&mut self) {
        debug!(self.logger, "Scanning for delinquencies");

        let now = SystemTime::now();
        self.receivable_dao
            .new_delinquencies(now, &PAYMENT_CURVES)
            .unwrap_or_else(|e| panic!("{}", e))
            .into_iter()
            .for_each(|account| {
                self.banned_dao.ban(&account.wallet);
                let (balance, age) = Self::balance_and_age(&account);
                info!(
                    self.logger,
                    "Wallet {} (balance: {} MASQ, age: {} sec) banned for delinquency",
                    account.wallet,
                    balance,
                    age.as_secs()
                )
            });

        self.receivable_dao
            .paid_delinquencies(&PAYMENT_CURVES)
            .unwrap_or_else(|e| panic!("{}", e))
            .into_iter()
            .for_each(|account| {
                self.banned_dao.unban(&account.wallet);
                let (balance, age) = Self::balance_and_age(&account);
                info!(
                    self.logger,
                    "Wallet {} (balance: {} MASQ, age: {} sec) is no longer delinquent: unbanned",
                    account.wallet,
                    balance,
                    age.as_secs()
                )
            });
    }

    fn scan_for_received_payments(&mut self) {
        let future_logger = self.logger.clone();
        debug!(
            self.logger,
            "Scanning for payments to {}", self.earning_wallet
        );
        let future_report_new_payments_sub = self.report_new_payments_sub.clone();
        let start_block = match self.persistent_configuration.start_block() {
            Ok(start_block) => start_block,
            Err(pce) => {
                error!(
                    self.logger,
                    "Could not retrieve start block: {:?} - aborting received-payment scan", pce
                );
                return;
            }
        };
        let future = self
            .retrieve_transactions_sub
            .as_ref()
            .expect("BlockchainBridge is unbound")
            .send(RetrieveTransactions {
                start_block,
                recipient: self.earning_wallet.clone(),
            })
            .then(move |transactions_possibly| match transactions_possibly {
                Ok(Ok(ref vec)) if vec.is_empty() => {
                    debug!(future_logger, "No payments detected");
                    Ok(())
                }
                Ok(Ok(transactions)) => {
                    future_report_new_payments_sub
                        .expect("Accountant is unbound")
                        .try_send(ReceivedPayments {
                            payments: transactions,
                        })
                        .expect("Accountant is dead.");
                    Ok(())
                }
                Ok(Err(e)) => {
                    warning!(
                        future_logger,
                        "Unable to retrieve transactions from Blockchain Bridge: {:?}",
                        e
                    );
                    Err(())
                }
                Err(e) => {
                    error!(
                        future_logger,
                        "Unable to send to Blockchain Bridge: {:?}", e
                    );
                    thread::sleep(Duration::from_secs(1));
                    panic!("Unable to send to Blockchain Bridge: {:?}", e);
                }
            });
        actix::spawn(future);
    }

    fn balance_and_age(account: &ReceivableAccount) -> (String, Duration) {
        let balance = format!("{}", (account.balance as f64) / 1_000_000_000.0); //balance in gwei
        let age = account
            .last_received_timestamp
            .elapsed()
            .unwrap_or_else(|_| Duration::new(0, 0));
        (balance, age)
    }

    fn should_pay(payable: &PayableAccount) -> bool {
        Self::payable_exceeded_threshold(payable).is_some()
    }

    fn payable_exceeded_threshold(payable: &PayableAccount) -> Option<u64> {
        // TODO: This calculation should be done in the database, if possible
        let time_since_last_paid = SystemTime::now()
            .duration_since(payable.last_paid_timestamp)
            .expect("Internal error")
            .as_secs();

        if time_since_last_paid <= PAYMENT_CURVES.payment_suggested_after_sec as u64 {
            return None;
        }

        if payable.balance
            <= u128_to_signed(PAYMENT_CURVES.permanent_debt_allowed_wei)
                .expect("we allowed an overflow")
        {
            //TODO handle this a better way
            return None;
        }

        let threshold = Accountant::calculate_payout_threshold(time_since_last_paid);
        if payable.balance as f64 > threshold {
            Some(threshold as u64)
        } else {
            None
        }
    }

    fn calculate_payout_threshold(x: u64) -> f64 {
        let m = -((PAYMENT_CURVES.balance_to_decrease_from_wei as f64
            - PAYMENT_CURVES.permanent_debt_allowed_wei as f64)
            / (PAYMENT_CURVES.balance_decreases_for_sec as f64
                - PAYMENT_CURVES.payment_suggested_after_sec as f64));
        let b = PAYMENT_CURVES.balance_to_decrease_from_wei as f64
            - m * PAYMENT_CURVES.payment_suggested_after_sec as f64;
        m * x as f64 + b
    }

    fn record_service_provided(
        &self,
        service_rate: u64,
        byte_rate: u64,
        payload_size: usize,
        wallet: &Wallet,
    ) {
        let byte_charge = byte_rate as u128 * (payload_size as u128);
        let total_charge = service_rate as u128 + byte_charge;
        if !self.our_wallet(wallet) {
            match self.receivable_dao
                .as_ref()
                .more_money_receivable(wallet, total_charge, &InsertUpdateCoreReal) {
                Ok(_) => (),
                Err(e) => error! (
                    self.logger,
                    "Overflow error trying to record service provided to Node with consuming wallet {}: service rate {}, byte rate {}, payload size {}. Skipping",
                    wallet,
                    service_rate,
                    byte_rate,
                    payload_size
                ),
            };
        } else {
            info!(
                self.logger,
                "Not recording service provided for our wallet {}", wallet
            );
        }
    }

    fn record_service_consumed(
        &self,
        service_rate: u64,
        byte_rate: u64,
        payload_size: usize,
        wallet: &Wallet,
    ) {
        let byte_charge = byte_rate as u128 * (payload_size as u128);
        let total_charge = service_rate as u128 + byte_charge;
        if !self.our_wallet(wallet) {
            match self.payable_dao
                .as_ref()
                .more_money_payable(wallet, total_charge,&InsertUpdateCoreReal) {
                Ok(_) => (),
                //TODO extend this - it may not be only overflow
                Err(e) => error! (
                    self.logger,
                    "Overflow error trying to record service consumed from Node with earning wallet {}: service rate {}, byte rate {}, payload size {}. Skipping",
                    wallet,
                    service_rate,
                    byte_rate,
                    payload_size
                ),
            };
        } else {
            info!(
                self.logger,
                "Not recording service consumed to our wallet {}", wallet
            );
        }
    }

    fn our_wallet(&self, wallet: &Wallet) -> bool {
        match &self.consuming_wallet {
            Some(ref consuming) if consuming.address() == wallet.address() => true,
            _ => wallet.address() == self.earning_wallet.address(),
        }
    }

    //for debugging only
    fn investigate_debt_extremes(all_non_pending_payables: &[PayableAccount]) -> String {
        if all_non_pending_payables.is_empty() {
            "Payable scan found no debts".to_string()
        } else {
            struct PayableInfo {
                balance: i128,
                age: Duration,
            }
            let now = SystemTime::now();
            let init = (
                PayableInfo {
                    balance: 0,
                    age: Duration::ZERO,
                },
                PayableInfo {
                    balance: 0,
                    age: Duration::ZERO,
                },
            );
            let (biggest, oldest) = all_non_pending_payables.iter().fold(init, |sofar, p| {
                let (mut biggest, mut oldest) = sofar;
                let p_age = now
                    .duration_since(p.last_paid_timestamp)
                    .expect("Payable time is corrupt");
                {
                    //seek for a test for this if you don't understand the purpose
                    let check_age_significance_across =
                        || -> bool { p.balance == biggest.balance && p_age > biggest.age };
                    if p.balance > biggest.balance || check_age_significance_across() {
                        biggest = PayableInfo {
                            balance: p.balance,
                            age: p_age,
                        }
                    }
                    let check_balance_significance_across =
                        || -> bool { p_age == oldest.age && p.balance > oldest.balance };
                    if p_age > oldest.age || check_balance_significance_across() {
                        oldest = PayableInfo {
                            balance: p.balance,
                            age: p_age,
                        }
                    }
                }
                (biggest, oldest)
            });
            format!("Payable scan found {} debts; the biggest is {} owed for {}sec, the oldest is {} owed for {}sec",
                    all_non_pending_payables.len(), biggest.balance, biggest.age.as_secs(),
                    oldest.balance, oldest.age.as_secs())
        }
    }

    fn payments_debug_summary(qualified_payables: &[PayableAccount]) -> String {
        let now = SystemTime::now();
        let list = qualified_payables
            .iter()
            .map(|payable| {
                let p_age = now
                    .duration_since(payable.last_paid_timestamp)
                    .expect("Payable time is corrupt");
                let threshold =
                    Self::payable_exceeded_threshold(payable).expect("Threshold suddenly changed!");
                format!(
                    "{} owed for {}sec exceeds threshold: {}; creditor: {}",
                    payable.balance,
                    p_age.as_secs(),
                    threshold,
                    payable.wallet
                )
            })
            .join("\n");
        String::from("Paying qualified debts:\n").add(&list)
    }

    fn multi_insert_payment_log_error(
        payments: &[Transaction],
        logger: &Logger,
        error: AccountantError,
    ) {
        let mut report_lines = vec![format!("{:10} {:42} {:18}", "Block #", "Wallet", "Amount")];
        let mut sum = 0_u128;
        payments.iter().for_each(|t| {
            report_lines.push(format!(
                "{:10} {:42} {:18}",
                t.block_number, t.from, t.wei_amount
            ));
            sum += t.wei_amount;
        });
        report_lines.push(format!("{:10} {:42} {:18}", "TOTAL", "", sum));
        let report = report_lines.join("\n");
        error!(
            logger,
            "Payment reception failed, rolling back: Multi insert payment: {:?}\n{}", error, report
        );
    }

    fn handle_bind_message(&mut self, msg: BindMessage) {
        self.report_accounts_payable_sub =
            Some(msg.peer_actors.blockchain_bridge.report_accounts_payable);
        self.retrieve_transactions_sub =
            Some(msg.peer_actors.blockchain_bridge.retrieve_transactions);
        self.report_new_payments_sub = Some(msg.peer_actors.accountant.report_new_payments);
        self.report_sent_payments_sub = Some(msg.peer_actors.accountant.report_sent_payments);
        self.ui_message_sub = Some(msg.peer_actors.ui_gateway.node_to_ui_message_sub);

        info!(self.logger, "Accountant bound");
    }

    fn handle_start_message(&mut self) {
        self.scan_for_payables();
        self.scan_for_received_payments();
        self.scan_for_delinquencies();
    }

    fn handle_received_payments(&mut self, received_payments: ReceivedPayments) {
        self.receivable_dao
            .as_mut()
            .more_money_received(&received_payments.payments, &InsertUpdateCoreReal)
            .unwrap_or_else(|e| {
                Self::multi_insert_payment_log_error(&received_payments.payments, &self.logger, e)
            })
    }

    fn handle_sent_payments(&mut self, sent_payments: SentPayments) {
        sent_payments
            .payments
            .iter()
            .for_each(|payment| match payment {
                Ok(payment) => match self
                    .payable_dao
                    .as_mut()
                    .payment_sent(payment, &InsertUpdateCoreReal)
                {
                    Ok(()) => (),
                    Err(e) => error!(
                        self.logger,
                        "{}; amount {} to address {} on transaction {}",
                        e,
                        payment.amount,
                        payment.to,
                        payment.transaction,
                    ),
                },
                Err(e) => warning!(
                    self.logger,
                    "{} Please check your blockchain service URL configuration.",
                    e
                ),
            })
    }

    fn handle_report_routing_service_provided_message(
        &mut self,
        msg: ReportRoutingServiceProvidedMessage,
    ) {
        debug!(
            self.logger,
            "Charging routing of {} bytes to wallet {}", msg.payload_size, msg.paying_wallet
        );
        self.record_service_provided(
            msg.service_rate,
            msg.byte_rate,
            msg.payload_size,
            &msg.paying_wallet,
        );
    }

    fn handle_report_exit_service_provided_message(
        &mut self,
        msg: ReportExitServiceProvidedMessage,
    ) {
        debug!(
            self.logger,
            "Charging exit service for {} bytes to wallet {} at {} per service and {} per byte",
            msg.payload_size,
            msg.paying_wallet,
            msg.service_rate,
            msg.byte_rate
        );
        self.record_service_provided(
            msg.service_rate,
            msg.byte_rate,
            msg.payload_size,
            &msg.paying_wallet,
        );
    }

    fn handle_report_routing_service_consumed_message(
        &mut self,
        msg: ReportRoutingServiceConsumedMessage,
    ) {
        debug!(
            self.logger,
            "Accruing debt to wallet {} for consuming routing service {} bytes",
            msg.earning_wallet,
            msg.payload_size
        );
        self.record_service_consumed(
            msg.service_rate,
            msg.byte_rate,
            msg.payload_size,
            &msg.earning_wallet,
        );
    }

    fn handle_report_exit_service_consumed_message(
        &mut self,
        msg: ReportExitServiceConsumedMessage,
    ) {
        debug!(
            self.logger,
            "Accruing debt to wallet {} for consuming exit service {} bytes",
            msg.earning_wallet,
            msg.payload_size
        );
        self.record_service_consumed(
            msg.service_rate,
            msg.byte_rate,
            msg.payload_size,
            &msg.earning_wallet,
        );
    }

    fn handle_node_from_ui_message(&mut self, msg: NodeFromUiMessage) {
        let client_id = msg.client_id;
        let result: Result<(UiFinancialsRequest, u64), UiMessageError> =
            UiFinancialsRequest::fmb(msg.body);
        match result {
            Ok((payload, context_id)) => self.handle_financials(client_id, context_id, payload),
            Err(UnexpectedMessage(opcode, path)) => debug!(
                &self.logger,
                "Ignoring {:?} request from client {} with opcode '{}'", path, client_id, opcode
            ),
            Err(e) => panic!("Received obsolete error: {:?}", e),
        }
    }

    fn handle_financials(&mut self, client_id: u64, context_id: u64, request: UiFinancialsRequest) {
        let payables = self
            .payable_dao
            .top_records(
                u128_to_signed(request.payable_minimum_amount).unwrap_or_else(|e| unimplemented!()),
                request.payable_maximum_age,
            )
            .unwrap_or_else(|e| panic!("{}", e))
            .iter()
            .map(|account| UiPayableAccount {
                wallet: account.wallet.to_string(),
                age: SystemTime::now()
                    .duration_since(account.last_paid_timestamp)
                    .expect("Bad interval")
                    .as_secs(),
                amount: account.balance as u64,
                pending_transaction: account
                    .pending_payment_transaction
                    .map(|ppt| format!("0x{:0X}", ppt)),
            })
            .collect_vec();
        let total_payable = match self.payable_dao.total(&TotalInnerEncapsulationPayableReal) {
            Ok(total) => total,
            Err(e) => unimplemented!(),
        };
        let receivables = self
            .receivable_dao
            .top_records(
                request.receivable_minimum_amount,
                request.receivable_maximum_age,
            )
            .unwrap_or_else(|e| panic!("{}", e))
            .iter()
            .map(|account| UiReceivableAccount {
                wallet: account.wallet.to_string(),
                age: SystemTime::now()
                    .duration_since(account.last_received_timestamp)
                    .expect("Bad interval")
                    .as_secs(),
                amount: account.balance as u64,
            })
            .collect_vec();
        let total_receivable = self.receivable_dao.total();
        let body = UiFinancialsResponse {
            payables,
            total_payable,
            receivables,
            total_receivable,
        }
        .tmb(context_id);
        self.ui_message_sub
            .as_ref()
            .expect("UiGateway not bound")
            .try_send(NodeToUiMessage {
                target: ClientId(client_id),
                body,
            })
            .expect("UiGateway is dead");
    }
}
//TODO I think this is needless

pub fn u128_to_signed(value: u128) -> Result<i128, SignConversionError> {
    i128::try_from(value).map_err(|e| {
        SignConversionError::U128(format!(
            "conversion of {} from u128 to i128 failed on: {}",
            value, e
        ))
    })
}

pub fn i128_to_unsigned(value: i128) -> Result<u128, SignConversionError> {
    u128::try_from(value).map_err(|e| {
        SignConversionError::I128(format!(
            "conversion of {} from i128 to u128 failed on: {}",
            value, e
        ))
    })
}

pub fn u64_to_signed(value: u64) -> Result<i64, SignConversionError> {
    i64::try_from(value).map_err(|e| {
        SignConversionError::U64(format!(
            "conversion of {} from u64 to i64 failed on: {}",
            value, e
        ))
    })
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::accountant::test_utils::{
        make_accountant, make_receivable_account, BannedDaoFactoryMock, BannedDaoMock,
        ConfigDaoFactoryMock, PayableDaoFactoryMock, PayableDaoMock, ReceivableDaoFactoryMock,
        ReceivableDaoMock,
    };
    use crate::blockchain::blockchain_interface::BlockchainError;
    use crate::blockchain::blockchain_interface::Transaction;
    use crate::database::dao_utils::from_time_t;
    use crate::database::dao_utils::to_time_t;
    use crate::database::db_initializer::test_utils::ConnectionWrapperMock;
    use crate::database::db_initializer::{DbInitializer, DbInitializerReal};
    use crate::db_config::mocks::ConfigDaoMock;
    use crate::db_config::persistent_configuration::PersistentConfigError;
    use crate::sub_lib::accountant::ReportRoutingServiceConsumedMessage;
    use crate::sub_lib::blockchain_bridge::ReportAccountsPayable;
    use crate::sub_lib::wallet::Wallet;
    use crate::test_utils::logging::init_test_logging;
    use crate::test_utils::logging::TestLogHandler;
    use crate::test_utils::persistent_configuration_mock::PersistentConfigurationMock;
    use crate::test_utils::recorder::make_recorder;
    use crate::test_utils::recorder::peer_actors_builder;
    use crate::test_utils::recorder::Recorder;
    use crate::test_utils::{logging, make_wallet};
    use actix::System;
    use ethereum_types::BigEndianHash;
    use ethsign_crypto::Keccak256;
    use masq_lib::blockchains::chains::Chain;
    use masq_lib::test_utils::utils::ensure_node_home_directory_exists;
    use masq_lib::ui_gateway::MessagePath::{Conversation, FireAndForget};
    use masq_lib::ui_gateway::{MessageBody, MessageTarget, NodeFromUiMessage, NodeToUiMessage};
    use rusqlite::Error;
    use std::cell::RefCell;
    use std::convert::TryFrom;
    use std::ops::Sub;
    use std::rc::Rc;
    use std::sync::Mutex;
    use std::sync::{Arc, MutexGuard};
    use std::thread;
    use std::time::Duration;
    use std::time::SystemTime;
    use web3::types::H256;
    use web3::types::U256;

    #[test]
    fn new_calls_factories_properly() {
        let config = BootstrapperConfig::new();
        let payable_dao_factory_called = Rc::new(RefCell::new(false));
        let payable_dao = PayableDaoMock::new();
        let payable_dao_factory =
            PayableDaoFactoryMock::new(payable_dao).called(&payable_dao_factory_called);
        let receivable_dao_factory_called = Rc::new(RefCell::new(false));
        let receivable_dao = ReceivableDaoMock::new();
        let receivable_dao_factory =
            ReceivableDaoFactoryMock::new(receivable_dao).called(&receivable_dao_factory_called);
        let banned_dao_factory_called = Rc::new(RefCell::new(false));
        let banned_dao = BannedDaoMock::new();
        let banned_dao_factory =
            BannedDaoFactoryMock::new(banned_dao).called(&banned_dao_factory_called);
        let config_dao_factory_called = Rc::new(RefCell::new(false));
        let config_dao = ConfigDaoMock::new();
        let config_dao_factory =
            ConfigDaoFactoryMock::new(config_dao).called(&config_dao_factory_called);

        let _ = Accountant::new(
            &config,
            Box::new(payable_dao_factory),
            Box::new(receivable_dao_factory),
            Box::new(banned_dao_factory),
            Box::new(config_dao_factory),
        );

        assert_eq!(payable_dao_factory_called.as_ref(), &RefCell::new(true));
        assert_eq!(receivable_dao_factory_called.as_ref(), &RefCell::new(true));
        assert_eq!(banned_dao_factory_called.as_ref(), &RefCell::new(true));
        assert_eq!(config_dao_factory_called.as_ref(), &RefCell::new(true));
    }

    #[test]
    fn financials_request_produces_financials_response() {
        let payable_top_records_parameters_arc = Arc::new(Mutex::new(vec![]));
        let payable_dao = PayableDaoMock::new()
            .top_records_parameters(&payable_top_records_parameters_arc)
            .top_records_result(Ok(vec![
                PayableAccount {
                    wallet: make_wallet("earning 1"),
                    balance: 12345678,
                    last_paid_timestamp: SystemTime::now().sub(Duration::from_secs(10000)),
                    pending_payment_transaction: Some(H256::from_uint(&U256::from(123))),
                },
                PayableAccount {
                    wallet: make_wallet("earning 2"),
                    balance: 12345679,
                    last_paid_timestamp: SystemTime::now().sub(Duration::from_secs(10001)),
                    pending_payment_transaction: None,
                },
            ]))
            .total_result(Ok(23456789));
        let receivable_top_records_parameters_arc = Arc::new(Mutex::new(vec![]));
        let receivable_dao = ReceivableDaoMock::new()
            .top_records_parameters(&receivable_top_records_parameters_arc)
            .top_records_result(Ok(vec![
                ReceivableAccount {
                    wallet: make_wallet("consuming 1"),
                    balance: 87654321,
                    last_received_timestamp: SystemTime::now().sub(Duration::from_secs(20000)),
                },
                ReceivableAccount {
                    wallet: make_wallet("consuming 2"),
                    balance: 87654322,
                    last_received_timestamp: SystemTime::now().sub(Duration::from_secs(20001)),
                },
            ]))
            .total_result(98765432);
        let system = System::new("test");
        let subject = make_accountant(
            Some(bc_from_ac_plus_earning_wallet(
                AccountantConfig {
                    payable_scan_interval: Duration::from_millis(10_000),
                    payment_received_scan_interval: Duration::from_millis(10_000),
                },
                make_wallet("some_wallet_address"),
            )),
            Some(payable_dao),
            Some(receivable_dao),
            None,
            None,
        );
        let (ui_gateway, _, ui_gateway_recording_arc) = make_recorder();
        let subject_addr = subject.start();
        let peer_actors = peer_actors_builder().ui_gateway(ui_gateway).build();
        subject_addr.try_send(BindMessage { peer_actors }).unwrap();
        let ui_message = NodeFromUiMessage {
            client_id: 1234,
            body: MessageBody {
                opcode: "financials".to_string(),
                path: Conversation(2222),
                payload: Ok(r#"{"payableMinimumAmount": 50001, "payableMaximumAge": 50002, "receivableMinimumAmount": 50003, "receivableMaximumAge": 50004}"#.to_string()),
            }
        };

        subject_addr.try_send(ui_message).unwrap();

        System::current().stop();
        system.run();
        let payable_top_records_parameters = payable_top_records_parameters_arc.lock().unwrap();
        assert_eq!(*payable_top_records_parameters, vec![(50001, 50002)]);
        let receivable_top_records_parameters =
            receivable_top_records_parameters_arc.lock().unwrap();
        assert_eq!(*receivable_top_records_parameters, vec![(50003, 50004)]);
        let ui_gateway_recording = ui_gateway_recording_arc.lock().unwrap();
        let response = ui_gateway_recording.get_record::<NodeToUiMessage>(0);
        assert_eq!(response.target, MessageTarget::ClientId(1234));
        assert_eq!(response.body.opcode, "financials".to_string());
        assert_eq!(response.body.path, Conversation(2222));
        let parsed_payload =
            serde_json::from_str::<UiFinancialsResponse>(&response.body.payload.as_ref().unwrap())
                .unwrap();
        assert_eq!(
            parsed_payload,
            UiFinancialsResponse {
                payables: vec![
                    UiPayableAccount {
                        wallet: "0x00000000000000000000006561726e696e672031".to_string(),
                        age: 10000,
                        amount: 12345678,
                        pending_transaction: Some(
                            "0x000000000000000000000000000000000000000000000000000000000000007B"
                                .to_string()
                        ),
                    },
                    UiPayableAccount {
                        wallet: "0x00000000000000000000006561726e696e672032".to_string(),
                        age: 10001,
                        amount: 12345679,
                        pending_transaction: None,
                    }
                ],
                total_payable: 23456789,
                receivables: vec![
                    UiReceivableAccount {
                        wallet: "0x000000000000000000636f6e73756d696e672031".to_string(),
                        age: 20000,
                        amount: 87654321,
                    },
                    UiReceivableAccount {
                        wallet: "0x000000000000000000636f6e73756d696e672032".to_string(),
                        age: 20001,
                        amount: 87654322,
                    }
                ],
                total_receivable: 98765432
            }
        );
    }

    #[test]
    fn unexpected_ui_message_is_ignored() {
        init_test_logging();
        let system = System::new("test");
        let subject = make_accountant(
            Some(bc_from_ac_plus_earning_wallet(
                AccountantConfig {
                    payable_scan_interval: Duration::from_millis(10_000),
                    payment_received_scan_interval: Duration::from_millis(10_000),
                },
                make_wallet("some_wallet_address"),
            )),
            None,
            None,
            None,
            None,
        );
        let (ui_gateway, _, ui_gateway_recording_arc) = make_recorder();
        let subject_addr = subject.start();
        let peer_actors = peer_actors_builder().ui_gateway(ui_gateway).build();
        subject_addr.try_send(BindMessage { peer_actors }).unwrap();

        subject_addr
            .try_send(NodeFromUiMessage {
                client_id: 1234,
                body: MessageBody {
                    opcode: "farple-prang".to_string(),
                    path: FireAndForget,
                    payload: Ok("{}".to_string()),
                },
            })
            .unwrap();

        System::current().stop();
        system.run();
        let ui_gateway_recording = ui_gateway_recording_arc.lock().unwrap();
        assert_eq!(ui_gateway_recording.len(), 0);
        TestLogHandler::new().exists_log_containing(
            "DEBUG: Accountant: Ignoring FireAndForget request from client 1234 with opcode 'farple-prang'",
        );
    }

    #[test]
    fn accountant_calls_payable_dao_payment_sent_when_sent_payments() {
        let payment_sent_parameters = Arc::new(Mutex::new(vec![]));
        let payment_sent_parameters_inner = payment_sent_parameters.clone();

        let payable_dao = PayableDaoMock::new()
            .non_pending_payables_result(vec![])
            .payment_sent_parameters(payment_sent_parameters_inner)
            .payment_sent_result(Ok(()));

        let system = System::new("accountant_calls_payable_dao_payment_sent_when_sent_payments");

        let accountant = make_accountant(
            Some(bc_from_ac_plus_earning_wallet(
                AccountantConfig {
                    payable_scan_interval: Duration::from_millis(100),
                    payment_received_scan_interval: Duration::from_secs(10_000),
                },
                make_wallet("some_wallet_address"),
            )),
            Some(payable_dao),
            None,
            None,
            None,
        );

        let expected_wallet = make_wallet("paying_you");
        let expected_amount = 1;
        let expected_hash = H256::from("transaction_hash".keccak256());
        let mut expected_payment = Payment::new(
            expected_wallet.clone(),
            expected_amount,
            expected_hash.clone(),
        );
        let send_payments = SentPayments {
            payments: vec![Ok(expected_payment.clone())],
        };

        let subject = accountant.start();

        subject
            .try_send(send_payments)
            .expect("unexpected actix error");
        System::current().stop();
        system.run();

        let sent_payment_to = payment_sent_parameters.lock().unwrap();
        let actual = sent_payment_to.get(0).unwrap();

        expected_payment.timestamp = actual.timestamp;
        assert_eq!(actual, &expected_payment);
    }

    #[test]
    fn accountant_logs_warning_when_handle_sent_payments_encounters_a_blockchain_error() {
        init_test_logging();
        let payable_dao = PayableDaoMock::new().non_pending_payables_result(vec![]);

        let system = System::new("accountant_calls_payable_dao_payment_sent_when_sent_payments");

        let accountant = make_accountant(
            Some(bc_from_ac_plus_earning_wallet(
                AccountantConfig {
                    payable_scan_interval: Duration::from_millis(100),
                    payment_received_scan_interval: Duration::from_secs(10_000),
                },
                make_wallet("some_wallet_address"),
            )),
            Some(payable_dao),
            None,
            None,
            None,
        );

        let send_payments = SentPayments {
            payments: vec![Err(BlockchainError::TransactionFailed(
                "Payment attempt failed".to_string(),
            ))],
        };

        let subject = accountant.start();

        subject
            .try_send(send_payments)
            .expect("unexpected actix error");
        System::current().stop();
        system.run();

        TestLogHandler::new().await_log_containing(
            r#"WARN: Accountant: Blockchain TransactionFailed("Payment attempt failed"). Please check your blockchain service URL configuration."#,
            1000,
        );
    }

    #[test]
    fn accountant_reports_sent_payments_when_blockchain_bridge_reports_account_payable() {
        let earning_wallet = make_wallet("earner3000");
        let now = to_time_t(SystemTime::now());
        let expected_wallet = make_wallet("blah");
        let expected_wallet_inner = expected_wallet.clone();
        let expected_amount =
            u128::try_from(PAYMENT_CURVES.permanent_debt_allowed_wei + 1000).unwrap();

        let expected_pending_payment_transaction = H256::from("transaction_hash".keccak256());
        let expected_pending_payment_transaction_inner =
            expected_pending_payment_transaction.clone();

        let payable_dao = PayableDaoMock::new()
            .non_pending_payables_result(vec![PayableAccount {
                wallet: expected_wallet.clone(),
                balance: u128_to_signed(PAYMENT_CURVES.permanent_debt_allowed_wei + 1000).unwrap(),
                last_paid_timestamp: from_time_t(
                    now - u64_to_signed(PAYMENT_CURVES.balance_decreases_for_sec).unwrap() - 10,
                ),
                pending_payment_transaction: None,
            }])
            .non_pending_payables_result(vec![]);

        let blockchain_bridge = Recorder::new()
            .report_accounts_payable_response(Ok(vec![Ok(Payment::new(
                expected_wallet_inner,
                expected_amount,
                expected_pending_payment_transaction_inner,
            ))]))
            .retrieve_transactions_response(Ok(vec![]));

        let (accountant_mock, accountant_mock_awaiter, accountant_recording_arc) = make_recorder();

        thread::spawn(move || {
            let system = System::new(
                "accountant_reports_sent_payments_when_blockchain_bridge_reports_account_payable",
            );

            let peer_actors = peer_actors_builder()
                .blockchain_bridge(blockchain_bridge)
                .accountant(accountant_mock)
                .build();
            let subject = make_accountant(
                Some(bc_from_ac_plus_earning_wallet(
                    AccountantConfig {
                        payable_scan_interval: Duration::from_millis(100),
                        payment_received_scan_interval: Duration::from_secs(10_000),
                    },
                    earning_wallet.clone(),
                )),
                Some(payable_dao),
                None,
                None,
                None,
            );
            let subject_addr = subject.start();
            let accountant_subs = Accountant::make_subs_from(&subject_addr);

            send_bind_message!(accountant_subs, peer_actors);
            send_start_message!(accountant_subs);

            system.run();
        });

        accountant_mock_awaiter.await_message_count(1);

        let accountant_recording = accountant_recording_arc.lock().unwrap();
        let actual_payments = accountant_recording.get_record::<SentPayments>(0);
        let mut expected_payment = Payment::new(
            expected_wallet,
            expected_amount,
            expected_pending_payment_transaction,
        );
        let payments = actual_payments.payments.clone();
        let maybe_payment = payments.get(0).clone();
        let result_payment = maybe_payment.unwrap().clone();
        expected_payment.timestamp = result_payment.unwrap().timestamp;
        assert_eq!(
            actual_payments,
            &SentPayments {
                payments: vec![Ok(expected_payment)]
            }
        );
    }

    #[test]
    fn accountant_logs_warn_when_blockchain_bridge_report_accounts_payable_errors() {
        init_test_logging();
        let earning_wallet = make_wallet("earner3000");
        let now = to_time_t(SystemTime::now());
        let expected_wallet = make_wallet("blockchain_bridge_error");

        let payable_dao = PayableDaoMock::new()
            .non_pending_payables_result(vec![PayableAccount {
                wallet: expected_wallet.clone(),
                balance: u128_to_signed(PAYMENT_CURVES.permanent_debt_allowed_wei + 1000).unwrap(),
                last_paid_timestamp: from_time_t(
                    now - u64_to_signed(PAYMENT_CURVES.balance_decreases_for_sec).unwrap() - 10,
                ),
                pending_payment_transaction: None,
            }])
            .non_pending_payables_result(vec![]);

        let blockchain_bridge = Recorder::new()
            .retrieve_transactions_response(Ok(vec![]))
            .report_accounts_payable_response(Err("Failed to send transaction".to_string()));

        let (accountant_mock, _, accountant_recording_arc) = make_recorder();

        thread::spawn(move || {
            let system = System::new(
                "accountant_reports_sent_payments_when_blockchain_bridge_reports_account_payable",
            );

            let peer_actors = peer_actors_builder()
                .blockchain_bridge(blockchain_bridge)
                .accountant(accountant_mock)
                .build();
            let subject = make_accountant(
                Some(bc_from_ac_plus_earning_wallet(
                    AccountantConfig {
                        payable_scan_interval: Duration::from_millis(100),
                        payment_received_scan_interval: Duration::from_secs(10_000),
                    },
                    earning_wallet.clone(),
                )),
                Some(payable_dao),
                None,
                None,
                None,
            );
            let subject_addr = subject.start();
            let subject_subs = Accountant::make_subs_from(&subject_addr);

            send_bind_message!(subject_subs, peer_actors);
            send_start_message!(subject_subs);

            system.run();
        });

        TestLogHandler::new()
            .await_log_containing("WARN: Accountant: Failed to send transaction", 1000u64);

        let accountant_recording = accountant_recording_arc.lock().unwrap();
        assert_eq!(0, accountant_recording.len());
    }

    #[test]
    fn accountant_payment_received_scan_timer_triggers_scanning_for_payments() {
        let paying_wallet = make_wallet("wallet0");
        let earning_wallet = make_wallet("earner3000");
        let amount = 42u128;
        let expected_transactions = vec![Transaction {
            block_number: 7u64,
            from: paying_wallet.clone(),
            wei_amount: amount,
        }];
        let blockchain_bridge =
            Recorder::new().retrieve_transactions_response(Ok(expected_transactions.clone()));
        let blockchain_bridge_awaiter = blockchain_bridge.get_awaiter();
        let blockchain_bridge_recording = blockchain_bridge.get_recording();
        let (accountant_mock, accountant_awaiter, accountant_recording_arc) = make_recorder();
        let config = bc_from_ac_plus_earning_wallet(
            AccountantConfig {
                payable_scan_interval: Duration::from_secs(10_000),
                payment_received_scan_interval: Duration::from_millis(100),
            },
            earning_wallet.clone(),
        );

        thread::spawn(move || {
            let system = System::new(
                "accountant_payment_received_scan_timer_triggers_scanning_for_payments",
            );
            let payable_dao = PayableDaoMock::new().non_pending_payables_result(vec![]);
            let receivable_dao = ReceivableDaoMock::new()
                .new_delinquencies_result(Ok(vec![]))
                .paid_delinquencies_result(Ok(vec![]));
            let config_mock = PersistentConfigurationMock::new().start_block_result(Ok(5));
            let subject = make_accountant(
                Some(config),
                Some(payable_dao),
                Some(receivable_dao),
                None,
                Some(config_mock),
            );
            let peer_actors = peer_actors_builder()
                .blockchain_bridge(blockchain_bridge)
                .accountant(accountant_mock)
                .build();
            let subject_addr: Addr<Accountant> = subject.start();
            let subject_subs = Accountant::make_subs_from(&subject_addr);

            send_bind_message!(subject_subs, peer_actors);
            send_start_message!(subject_subs);

            system.run();
        });

        blockchain_bridge_awaiter.await_message_count(1);
        let retrieve_transactions_recording = blockchain_bridge_recording.lock().unwrap();
        let retrieve_transactions_message =
            retrieve_transactions_recording.get_record::<RetrieveTransactions>(0);
        assert_eq!(
            &RetrieveTransactions {
                start_block: 5u64,
                recipient: earning_wallet,
            },
            retrieve_transactions_message
        );

        accountant_awaiter.await_message_count(1);
        let received_payments_recording = accountant_recording_arc.lock().unwrap();
        let received_payments_message =
            received_payments_recording.get_record::<ReceivedPayments>(0);
        assert_eq!(
            &ReceivedPayments {
                payments: expected_transactions
            },
            received_payments_message
        );
    }

    #[test]
    fn handle_received_payments_logs_when_no_payment_are_given() {
        logging::init_test_logging();
        let home_dir = ensure_node_home_directory_exists(
            "receivable_dao",
            "handle_received_payments_logs_when_no_payment_are_given",
        );
        let conn = DbInitializerReal::default()
            .initialize(&home_dir, Chain::PolyMainnet, true)
            .unwrap();
        let receivable_dao = ReceivableDaoReal::new(conn);
        let mut accountant = make_accountant(None, None, None, None, None);
        accountant.receivable_dao = Box::new(receivable_dao);
        let system = System::new("received_payments_test");
        let addr = accountant.start();
        let msg = ReceivedPayments { payments: vec![] };

        let _ = addr.try_send(msg).unwrap();

        System::current().stop();
        system.run();
        TestLogHandler::new().exists_log_containing(
            "ERROR: Accountant: Payment reception failed, rolling back: Multi insert payment: ReceivableError(Other(\"no payments given\"))",
        );
    }

    #[test]
    fn handle_received_payments_logs_when_transaction_fails() {
        logging::init_test_logging();
        let conn_mock =
            ConnectionWrapperMock::default().transaction_result(Err(Error::InvalidQuery));
        let receivable_dao = ReceivableDaoReal::new(Box::new(conn_mock));
        let mut accountant = make_accountant(None, None, None, None, None);
        accountant.receivable_dao = Box::new(receivable_dao);
        let system = System::new("received_payments_test");
        let addr = accountant.start();
        let payments = vec![
            Transaction {
                block_number: 1234567890,
                from: Wallet::new("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                wei_amount: 123456789123456789,
            },
            Transaction {
                block_number: 2345678901,
                from: Wallet::new("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
                wei_amount: 234567891234567891,
            },
            Transaction {
                block_number: 3456789012,
                from: Wallet::new("0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
                wei_amount: 345678912345678912,
            },
        ];
        let msg = ReceivedPayments { payments };

        let _ = addr.try_send(msg).unwrap();

        System::current().stop();
        system.run();
        TestLogHandler::new().exists_log_containing(&format!(
            "ERROR: Accountant: Payment reception failed, rolling back: Multi insert payment: ReceivableError(Other(\"Query is not read-only\"))\n\
            Block #    Wallet                                     Amount            \n\
            1234567890 0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 123456789123456789\n\
            2345678901 0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 234567891234567891\n\
            3456789012 0xcccccccccccccccccccccccccccccccccccccccc 345678912345678912\n\
            TOTAL                                                 703703592703703592"
        ));
    }

    #[test]
    fn accountant_logs_if_no_transactions_were_detected() {
        init_test_logging();
        let earning_wallet = make_wallet("earner3000");
        let blockchain_bridge = Recorder::new().retrieve_transactions_response(Ok(vec![]));
        let blockchain_bridge_awaiter = blockchain_bridge.get_awaiter();
        let blockchain_bridge_recording = blockchain_bridge.get_recording();
        let (accountant_mock, _, accountant_recording_arc) = make_recorder();
        let config = bc_from_ac_plus_earning_wallet(
            AccountantConfig {
                payable_scan_interval: Duration::from_secs(10_000),
                payment_received_scan_interval: Duration::from_millis(100),
            },
            earning_wallet.clone(),
        );

        thread::spawn(move || {
            let system = System::new("accountant_logs_if_no_transactions_were_detected");
            let payable_dao = PayableDaoMock::new().non_pending_payables_result(vec![]);
            let receivable_dao = ReceivableDaoMock::new()
                .new_delinquencies_result(Ok(vec![]))
                .paid_delinquencies_result(Ok(vec![]));
            let config_mock = PersistentConfigurationMock::new().start_block_result(Ok(5));
            let subject = make_accountant(
                Some(config),
                Some(payable_dao),
                Some(receivable_dao),
                None,
                Some(config_mock),
            );
            let peer_actors = peer_actors_builder()
                .blockchain_bridge(blockchain_bridge)
                .accountant(accountant_mock)
                .build();
            let subject_addr: Addr<Accountant> = subject.start();
            let subject_subs = Accountant::make_subs_from(&subject_addr);

            send_bind_message!(subject_subs, peer_actors);
            send_start_message!(subject_subs);

            system.run();
        });

        blockchain_bridge_awaiter.await_message_count(1);
        TestLogHandler::new().exists_log_containing("DEBUG: Accountant: Scanning for payments");
        let retrieve_transactions_recording = blockchain_bridge_recording.lock().unwrap();
        let retrieve_transactions_message =
            retrieve_transactions_recording.get_record::<RetrieveTransactions>(0);
        assert_eq!(
            &RetrieveTransactions {
                start_block: 5u64,
                recipient: earning_wallet,
            },
            retrieve_transactions_message
        );

        TestLogHandler::new().exists_log_containing("DEBUG: Accountant: No payments detected");
        let accountant_recording = accountant_recording_arc.lock().unwrap();
        assert_eq!(0, accountant_recording.len())
    }

    #[test]
    fn accountant_logs_error_when_blockchain_bridge_responds_with_error() {
        init_test_logging();
        let earning_wallet = make_wallet("earner3000");
        let blockchain_bridge = Recorder::new().retrieve_transactions_response(Err(
            BlockchainError::QueryFailed("really bad".to_string()),
        ));
        let blockchain_bridge_awaiter = blockchain_bridge.get_awaiter();
        let blockchain_bridge_recording = blockchain_bridge.get_recording();
        let config = bc_from_ac_plus_earning_wallet(
            AccountantConfig {
                payable_scan_interval: Duration::from_secs(10_000),
                payment_received_scan_interval: Duration::from_millis(100),
            },
            earning_wallet.clone(),
        );

        thread::spawn(move || {
            let system =
                System::new("accountant_logs_error_when_blockchain_bridge_responds_with_error");
            let payable_dao = PayableDaoMock::new().non_pending_payables_result(vec![]);
            let receivable_dao = ReceivableDaoMock::new()
                .new_delinquencies_result(Ok(vec![]))
                .paid_delinquencies_result(Ok(vec![]));
            let config_mock = PersistentConfigurationMock::new().start_block_result(Ok(0));
            let subject = make_accountant(
                Some(config),
                Some(payable_dao),
                Some(receivable_dao),
                None,
                Some(config_mock),
            );
            let peer_actors = peer_actors_builder()
                .blockchain_bridge(blockchain_bridge)
                .build();
            let subject_addr: Addr<Accountant> = subject.start();
            let subject_subs = Accountant::make_subs_from(&subject_addr);

            send_bind_message!(subject_subs, peer_actors);
            send_start_message!(subject_subs);

            system.run();
        });

        blockchain_bridge_awaiter.await_message_count(1);
        let retrieve_transactions_recording = blockchain_bridge_recording.lock().unwrap();
        let retrieve_transactions_message =
            retrieve_transactions_recording.get_record::<RetrieveTransactions>(0);
        assert_eq!(earning_wallet, retrieve_transactions_message.recipient);

        TestLogHandler::new().exists_log_containing(
            r#"WARN: Accountant: Unable to retrieve transactions from Blockchain Bridge: QueryFailed("really bad")"#,
        );
    }

    #[test]
    fn accountant_receives_new_payments_to_the_receivables_dao() {
        let wallet = make_wallet("wallet0");
        let earning_wallet = make_wallet("earner3000");
        let gwei_amount = 42u128;
        let expected_payment = Transaction {
            block_number: 7u64,
            from: wallet.clone(),
            wei_amount: gwei_amount,
        };
        let more_money_received_params_arc = Arc::new(Mutex::new(vec![]));
        let receivable_dao = ReceivableDaoMock::new()
            .more_money_received_parameters(&more_money_received_params_arc)
            .more_money_received_result(Ok(()))
            .more_money_received_result(Ok(()));
        let accountant = make_accountant(
            Some(bc_from_ac_plus_earning_wallet(
                AccountantConfig {
                    payable_scan_interval: Duration::from_secs(10_000),
                    payment_received_scan_interval: Duration::from_secs(10_000),
                },
                earning_wallet.clone(),
            )),
            Some(PayableDaoMock::new().non_pending_payables_result(vec![])),
            Some(receivable_dao),
            None,
            None,
        );

        let system = System::new("accountant_receives_new_payments_to_the_receivables_dao");
        let subject = accountant.start();

        subject
            .try_send(ReceivedPayments {
                payments: vec![expected_payment.clone(), expected_payment.clone()],
            })
            .expect("unexpected actix error");
        System::current().stop();
        system.run();
        let more_money_received_params = more_money_received_params_arc.lock().unwrap();
        assert_eq!(1, more_money_received_params.len());

        let more_money_received_params = more_money_received_params.get(0).unwrap();
        assert_eq!(2, more_money_received_params.len());

        let first_payment = more_money_received_params.get(0).unwrap();
        assert_eq!(expected_payment.from, first_payment.from);
        assert_eq!(gwei_amount, first_payment.wei_amount);
        let second_payment = more_money_received_params.get(1).unwrap();
        assert_eq!(expected_payment.from, second_payment.from);
        assert_eq!(gwei_amount, second_payment.wei_amount);
    }

    #[test]
    fn accountant_payable_scan_timer_triggers_scanning_for_payables() {
        init_test_logging();
        let (blockchain_bridge, blockchain_bridge_awaiter, _) = make_recorder();
        let blockchain_bridge = blockchain_bridge
            .retrieve_transactions_response(Ok(vec![]))
            .report_accounts_payable_response(Ok(vec![]));

        thread::spawn(move || {
            let system =
                System::new("accountant_payable_scan_timer_triggers_scanning_for_payables");
            let config = bc_from_ac_plus_earning_wallet(
                AccountantConfig {
                    payable_scan_interval: Duration::from_millis(100),
                    payment_received_scan_interval: Duration::from_secs(100),
                },
                make_wallet("hi"),
            );
            let now = to_time_t(SystemTime::now());
            // slightly above minimum balance, to the right of the curve (time intersection)
            let account0 = PayableAccount {
                wallet: make_wallet("wallet0"),
                balance: u128_to_signed(PAYMENT_CURVES.permanent_debt_allowed_wei + 1).unwrap(),
                last_paid_timestamp: from_time_t(
                    now - u64_to_signed(PAYMENT_CURVES.balance_decreases_for_sec - 10).unwrap(),
                ),
                pending_payment_transaction: None,
            };
            let account1 = PayableAccount {
                wallet: make_wallet("wallet1"),
                balance: u128_to_signed(PAYMENT_CURVES.permanent_debt_allowed_wei + 2).unwrap(),
                last_paid_timestamp: from_time_t(
                    now - u64_to_signed(PAYMENT_CURVES.balance_decreases_for_sec).unwrap() - 12,
                ),
                pending_payment_transaction: None,
            };
            let payable_dao = PayableDaoMock::new()
                .non_pending_payables_result(vec![account0, account1])
                .non_pending_payables_result(vec![]);
            let subject = make_accountant(Some(config), Some(payable_dao), None, None, None);
            let peer_actors = peer_actors_builder()
                .blockchain_bridge(blockchain_bridge)
                .build();
            let subject_addr: Addr<Accountant> = subject.start();
            let subject_subs = Accountant::make_subs_from(&subject_addr);

            send_bind_message!(subject_subs, peer_actors);
            send_start_message!(subject_subs);

            system.run();
        });

        blockchain_bridge_awaiter.await_message_count(1);
        TestLogHandler::new().exists_log_containing("DEBUG: Accountant: Scanning for payables");
    }

    #[test]
    fn accountant_scans_after_startup() {
        init_test_logging();
        let (blockchain_bridge, _, _) = make_recorder();

        let system = System::new("accountant_scans_after_startup");
        let config = bc_from_ac_plus_wallets(
            AccountantConfig {
                payable_scan_interval: Duration::from_secs(1000),
                payment_received_scan_interval: Duration::from_secs(1000),
            },
            make_wallet("buy"),
            make_wallet("hi"),
        );
        let subject = make_accountant(Some(config), None, None, None, None);
        let peer_actors = peer_actors_builder()
            .blockchain_bridge(blockchain_bridge)
            .build();
        let subject_addr: Addr<Accountant> = subject.start();
        let subject_subs = Accountant::make_subs_from(&subject_addr);

        send_bind_message!(subject_subs, peer_actors);
        send_start_message!(subject_subs);

        System::current().stop();
        system.run();

        let tlh = TestLogHandler::new();
        tlh.await_log_containing("DEBUG: Accountant: Scanning for payables", 1000u64);
        tlh.exists_log_containing(&format!(
            "DEBUG: Accountant: Scanning for payments to {}",
            make_wallet("hi")
        ));
        tlh.exists_log_containing("DEBUG: Accountant: Scanning for delinquencies");
    }

    #[test]
    fn scan_for_payables_message_does_not_trigger_payment_for_balances_below_the_curve() {
        init_test_logging();
        let config = bc_from_ac_plus_earning_wallet(
            AccountantConfig {
                payable_scan_interval: Duration::from_secs(100),
                payment_received_scan_interval: Duration::from_secs(1000),
            },
            make_wallet("mine"),
        );
        let now = to_time_t(SystemTime::now());
        let accounts = vec![
            // below minimum balance, to the right of time intersection (inside buffer zone)
            PayableAccount {
                wallet: make_wallet("wallet0"),
                balance: u128_to_signed(PAYMENT_CURVES.permanent_debt_allowed_wei - 1).unwrap(),
                last_paid_timestamp: from_time_t(
                    now - u64_to_signed(PAYMENT_CURVES.balance_decreases_for_sec).unwrap() - 10,
                ),
                pending_payment_transaction: None,
            },
            // above balance intersection, to the left of minimum time (inside buffer zone)
            PayableAccount {
                wallet: make_wallet("wallet1"),
                balance: u128_to_signed(PAYMENT_CURVES.balance_to_decrease_from_wei + 1).unwrap(),
                last_paid_timestamp: from_time_t(
                    now - u64_to_signed(PAYMENT_CURVES.payment_suggested_after_sec).unwrap() + 10,
                ),
                pending_payment_transaction: None,
            },
            // above minimum balance, to the right of minimum time (not in buffer zone, below the curve)
            PayableAccount {
                wallet: make_wallet("wallet2"),
                balance: u128_to_signed(PAYMENT_CURVES.balance_to_decrease_from_wei - 1000)
                    .unwrap(),
                last_paid_timestamp: from_time_t(
                    now - u64_to_signed(PAYMENT_CURVES.payment_suggested_after_sec).unwrap() - 1,
                ),
                pending_payment_transaction: None,
            },
        ];
        let payable_dao = PayableDaoMock::new()
            .non_pending_payables_result(accounts.clone())
            .non_pending_payables_result(vec![]);
        let (blockchain_bridge, _, blockchain_bridge_recordings_arc) = make_recorder();
        let system = System::new(
            "scan_for_payables_message_does_not_trigger_payment_for_balances_below_the_curve",
        );
        let blockchain_bridge_addr: Addr<Recorder> = blockchain_bridge.start();
        let report_accounts_payable_sub =
            blockchain_bridge_addr.recipient::<ReportAccountsPayable>();
        let mut subject = make_accountant(Some(config), Some(payable_dao), None, None, None);
        subject.report_accounts_payable_sub = Some(report_accounts_payable_sub);

        subject.scan_for_payables();

        System::current().stop_with_code(0);
        system.run();

        let blockchain_bridge_recordings = blockchain_bridge_recordings_arc.lock().unwrap();
        assert_eq!(blockchain_bridge_recordings.len(), 0);
    }

    #[test]
    fn scan_for_payables_message_triggers_payment_for_balances_over_the_curve() {
        init_test_logging();
        let config = bc_from_ac_plus_earning_wallet(
            AccountantConfig {
                payable_scan_interval: Duration::from_millis(100),
                payment_received_scan_interval: Duration::from_millis(1_000),
            },
            make_wallet("mine"),
        );
        let now = to_time_t(SystemTime::now());
        let accounts = vec![
            // slightly above minimum balance, to the right of the curve (time intersection)
            PayableAccount {
                wallet: make_wallet("wallet0"),
                balance: u128_to_signed(PAYMENT_CURVES.permanent_debt_allowed_wei + 1).unwrap(),
                last_paid_timestamp: from_time_t(
                    now - u64_to_signed(PAYMENT_CURVES.balance_decreases_for_sec).unwrap() - 10,
                ),
                pending_payment_transaction: None,
            },
            // slightly above the curve (balance intersection), to the right of minimum time
            PayableAccount {
                wallet: make_wallet("wallet1"),
                balance: u128_to_signed(PAYMENT_CURVES.balance_to_decrease_from_wei + 1).unwrap(),
                last_paid_timestamp: from_time_t(
                    now - u64_to_signed(PAYMENT_CURVES.payment_suggested_after_sec).unwrap() - 10,
                ),
                pending_payment_transaction: None,
            },
        ];
        let payable_dao = PayableDaoMock::default()
            .non_pending_payables_result(accounts.clone())
            .non_pending_payables_result(vec![]);
        let (mut blockchain_bridge, blockchain_bridge_awaiter, blockchain_bridge_recordings_arc) =
            make_recorder();
        blockchain_bridge = blockchain_bridge
            .retrieve_transactions_response(Ok(vec![]))
            .report_accounts_payable_response(Ok(vec![]));

        thread::spawn(move || {
            let system = System::new(
                "scan_for_payables_message_triggers_payment_for_balances_over_the_curve",
            );

            let peer_actors = peer_actors_builder()
                .blockchain_bridge(blockchain_bridge)
                .build();
            let subject = make_accountant(Some(config), Some(payable_dao), None, None, None);
            let subject_addr = subject.start();
            let accountant_subs = Accountant::make_subs_from(&subject_addr);

            send_bind_message!(accountant_subs, peer_actors);
            send_start_message!(accountant_subs);

            system.run();
        });

        blockchain_bridge_awaiter.await_message_count(1);
        let blockchain_bridge_recordings = blockchain_bridge_recordings_arc.lock().unwrap();
        assert_eq!(
            blockchain_bridge_recordings.get_record::<ReportAccountsPayable>(0),
            &ReportAccountsPayable { accounts }
        );
    }

    #[test]
    fn payment_received_scan_triggers_scan_for_delinquencies() {
        let ban_parameters_arc = Arc::new(Mutex::new(vec![]));
        let ban_parameters_arc_inner = ban_parameters_arc.clone();
        let blockchain_bridge = Recorder::new().retrieve_transactions_response(Ok(vec![]));
        thread::spawn(move || {
            let system = System::new("payment_received_scan_triggers_scan_for_delinquencies");
            let config = bc_from_ac_plus_earning_wallet(
                AccountantConfig {
                    payable_scan_interval: Duration::from_secs(10_000),
                    payment_received_scan_interval: Duration::from_millis(100),
                },
                make_wallet("hi"),
            );

            let payable_dao = PayableDaoMock::new().non_pending_payables_result(vec![]);
            let receivable_dao = ReceivableDaoMock::new()
                .new_delinquencies_result(Ok(vec![make_receivable_account(1234, true)]))
                .paid_delinquencies_result(Ok(vec![]));
            let banned_dao = BannedDaoMock::new()
                .ban_list_result(vec![])
                .ban_parameters(&ban_parameters_arc_inner);
            let subject = make_accountant(
                Some(config),
                Some(payable_dao),
                Some(receivable_dao),
                Some(banned_dao),
                None,
            );
            let peer_actors = peer_actors_builder()
                .blockchain_bridge(blockchain_bridge)
                .build();
            let subject_addr: Addr<Accountant> = subject.start();
            let subject_subs = Accountant::make_subs_from(&subject_addr);

            send_bind_message!(subject_subs, peer_actors);
            send_start_message!(subject_subs);

            system.run();
        });

        thread::sleep(Duration::from_millis(200));

        let ban_parameters = ban_parameters_arc.lock().unwrap();
        assert_eq!(
            "0x00000000000000000077616c6c65743132333464",
            &format!("{:#x}", &ban_parameters[0].address())
        );
    }

    #[test]
    fn scan_for_received_payments_handles_error_retrieving_start_block() {
        init_test_logging();
        let persistent_config = PersistentConfigurationMock::new()
            .start_block_result(Err(PersistentConfigError::NotPresent));
        let mut subject = make_accountant(None, None, None, None, Some(persistent_config));

        subject.scan_for_received_payments();

        let tlh = TestLogHandler::new();
        tlh.exists_log_matching("ERROR: Accountant: Could not retrieve start block: NotPresent - aborting received-payment scan");
    }

    #[test]
    fn scan_for_delinquencies_triggers_bans_and_unbans() {
        init_test_logging();
        let config = bc_from_ac_plus_earning_wallet(
            AccountantConfig {
                payable_scan_interval: Duration::from_secs(100),
                payment_received_scan_interval: Duration::from_secs(1000),
            },
            make_wallet("mine"),
        );
        let newly_banned_1 = make_receivable_account(1234, true);
        let newly_banned_2 = make_receivable_account(2345, true);
        let newly_unbanned_1 = make_receivable_account(3456, false);
        let newly_unbanned_2 = make_receivable_account(4567, false);
        let payable_dao = PayableDaoMock::new().non_pending_payables_result(vec![]);
        let new_delinquencies_parameters_arc = Arc::new(Mutex::new(vec![]));
        let paid_delinquencies_parameters_arc = Arc::new(Mutex::new(vec![]));
        let receivable_dao = ReceivableDaoMock::new()
            .new_delinquencies_parameters(&new_delinquencies_parameters_arc)
            .new_delinquencies_result(Ok(vec![newly_banned_1.clone(), newly_banned_2.clone()]))
            .paid_delinquencies_parameters(&paid_delinquencies_parameters_arc)
            .paid_delinquencies_result(Ok(vec![
                newly_unbanned_1.clone(),
                newly_unbanned_2.clone(),
            ]));
        let ban_parameters_arc = Arc::new(Mutex::new(vec![]));
        let unban_parameters_arc = Arc::new(Mutex::new(vec![]));
        let banned_dao = BannedDaoMock::new()
            .ban_list_result(vec![])
            .ban_parameters(&ban_parameters_arc)
            .unban_parameters(&unban_parameters_arc);
        let mut subject = make_accountant(
            Some(config),
            Some(payable_dao),
            Some(receivable_dao),
            Some(banned_dao),
            None,
        );

        subject.scan_for_delinquencies();

        let new_delinquencies_parameters: MutexGuard<Vec<(SystemTime, PaymentCurves)>> =
            new_delinquencies_parameters_arc.lock().unwrap();
        assert_eq!(PAYMENT_CURVES.clone(), new_delinquencies_parameters[0].1);
        let paid_delinquencies_parameters: MutexGuard<Vec<PaymentCurves>> =
            paid_delinquencies_parameters_arc.lock().unwrap();
        assert_eq!(PAYMENT_CURVES.clone(), paid_delinquencies_parameters[0]);
        let ban_parameters = ban_parameters_arc.lock().unwrap();
        assert!(ban_parameters.contains(&newly_banned_1.wallet));
        assert!(ban_parameters.contains(&newly_banned_2.wallet));
        assert_eq!(2, ban_parameters.len());
        let unban_parameters = unban_parameters_arc.lock().unwrap();
        assert!(unban_parameters.contains(&newly_unbanned_1.wallet));
        assert!(unban_parameters.contains(&newly_unbanned_2.wallet));
        assert_eq!(2, unban_parameters.len());
        let tlh = TestLogHandler::new();
        tlh.exists_log_matching("INFO: Accountant: Wallet 0x00000000000000000077616c6c65743132333464 \\(balance: 1234 MASQ, age: \\d+ sec\\) banned for delinquency");
        tlh.exists_log_matching("INFO: Accountant: Wallet 0x00000000000000000077616c6c65743233343564 \\(balance: 2345 MASQ, age: \\d+ sec\\) banned for delinquency");
        tlh.exists_log_matching("INFO: Accountant: Wallet 0x00000000000000000077616c6c6574333435366e \\(balance: 3456 MASQ, age: \\d+ sec\\) is no longer delinquent: unbanned");
        tlh.exists_log_matching("INFO: Accountant: Wallet 0x00000000000000000077616c6c6574343536376e \\(balance: 4567 MASQ, age: \\d+ sec\\) is no longer delinquent: unbanned");
    }

    #[test]
    fn report_routing_service_provided_message_is_received() {
        init_test_logging();
        let config = bc_from_ac_plus_earning_wallet(
            AccountantConfig {
                payable_scan_interval: Duration::from_secs(100),
                payment_received_scan_interval: Duration::from_secs(100),
            },
            make_wallet("hi"),
        );
        let more_money_receivable_parameters_arc = Arc::new(Mutex::new(vec![]));
        let payable_dao_mock = PayableDaoMock::new().non_pending_payables_result(vec![]);
        let receivable_dao_mock = ReceivableDaoMock::new()
            .more_money_receivable_parameters(&more_money_receivable_parameters_arc)
            .more_money_receivable_result(Ok(()));
        let subject = make_accountant(
            Some(config),
            Some(payable_dao_mock),
            Some(receivable_dao_mock),
            None,
            None,
        );
        let system = System::new("report_routing_service_message_is_received");
        let subject_addr: Addr<Accountant> = subject.start();
        subject_addr
            .try_send(BindMessage {
                peer_actors: peer_actors_builder().build(),
            })
            .unwrap();

        let paying_wallet = make_wallet("booga");
        subject_addr
            .try_send(ReportRoutingServiceProvidedMessage {
                paying_wallet: paying_wallet.clone(),
                payload_size: 1234,
                service_rate: 42,
                byte_rate: 24,
            })
            .unwrap();

        System::current().stop_with_code(0);
        system.run();
        let more_money_receivable_parameters = more_money_receivable_parameters_arc.lock().unwrap();
        assert_eq!(
            more_money_receivable_parameters[0],
            (make_wallet("booga"), (1 * 42) + (1234 * 24))
        );
        TestLogHandler::new().exists_log_containing(&format!(
            "DEBUG: Accountant: Charging routing of 1234 bytes to wallet {}",
            paying_wallet
        ));
    }

    #[test]
    fn report_routing_service_provided_message_is_received_from_our_consuming_wallet() {
        init_test_logging();
        let consuming_wallet = make_wallet("our consuming wallet");
        let config = bc_from_ac_plus_wallets(
            AccountantConfig {
                payable_scan_interval: Duration::from_secs(100),
                payment_received_scan_interval: Duration::from_secs(100),
            },
            consuming_wallet.clone(),
            make_wallet("our earning wallet"),
        );
        let more_money_receivable_parameters_arc = Arc::new(Mutex::new(vec![]));
        let payable_dao_mock = PayableDaoMock::new().non_pending_payables_result(vec![]);
        let receivable_dao_mock = ReceivableDaoMock::new()
            .more_money_receivable_parameters(&more_money_receivable_parameters_arc);
        let subject = make_accountant(
            Some(config),
            Some(payable_dao_mock),
            Some(receivable_dao_mock),
            None,
            None,
        );
        let system = System::new("report_routing_service_message_is_received");
        let subject_addr: Addr<Accountant> = subject.start();
        subject_addr
            .try_send(BindMessage {
                peer_actors: peer_actors_builder().build(),
            })
            .unwrap();

        subject_addr
            .try_send(ReportRoutingServiceProvidedMessage {
                paying_wallet: consuming_wallet.clone(),
                payload_size: 1234,
                service_rate: 42,
                byte_rate: 24,
            })
            .unwrap();

        System::current().stop_with_code(0);
        system.run();
        assert!(more_money_receivable_parameters_arc
            .lock()
            .unwrap()
            .is_empty());

        TestLogHandler::new().exists_log_containing(&format!(
            "INFO: Accountant: Not recording service provided for our wallet {}",
            consuming_wallet,
        ));
    }

    #[test]
    fn report_routing_service_provided_message_is_received_from_our_earning_wallet() {
        init_test_logging();
        let earning_wallet = make_wallet("our earning wallet");
        let config = bc_from_ac_plus_earning_wallet(
            AccountantConfig {
                payable_scan_interval: Duration::from_secs(100),
                payment_received_scan_interval: Duration::from_secs(100),
            },
            earning_wallet.clone(),
        );
        let more_money_receivable_parameters_arc = Arc::new(Mutex::new(vec![]));
        let payable_dao_mock = PayableDaoMock::new().non_pending_payables_result(vec![]);
        let receivable_dao_mock = ReceivableDaoMock::new()
            .more_money_receivable_parameters(&more_money_receivable_parameters_arc);
        let subject = make_accountant(
            Some(config),
            Some(payable_dao_mock),
            Some(receivable_dao_mock),
            None,
            None,
        );
        let system = System::new("report_routing_service_message_is_received");
        let subject_addr: Addr<Accountant> = subject.start();
        subject_addr
            .try_send(BindMessage {
                peer_actors: peer_actors_builder().build(),
            })
            .unwrap();

        subject_addr
            .try_send(ReportRoutingServiceProvidedMessage {
                paying_wallet: earning_wallet.clone(),
                payload_size: 1234,
                service_rate: 42,
                byte_rate: 24,
            })
            .unwrap();

        System::current().stop_with_code(0);
        system.run();
        assert!(more_money_receivable_parameters_arc
            .lock()
            .unwrap()
            .is_empty());

        TestLogHandler::new().exists_log_containing(&format!(
            "INFO: Accountant: Not recording service provided for our wallet {}",
            earning_wallet,
        ));
    }

    #[test]
    fn report_routing_service_consumed_message_is_received() {
        init_test_logging();
        let config = bc_from_ac_plus_earning_wallet(
            AccountantConfig {
                payable_scan_interval: Duration::from_secs(100),
                payment_received_scan_interval: Duration::from_secs(100),
            },
            make_wallet("hi"),
        );
        let more_money_payable_parameters_arc = Arc::new(Mutex::new(vec![]));
        let payable_dao_mock = PayableDaoMock::new()
            .non_pending_payables_result(vec![])
            .more_money_payable_parameters(more_money_payable_parameters_arc.clone())
            .more_money_payable_result(Ok(()));
        let subject = make_accountant(Some(config), Some(payable_dao_mock), None, None, None);
        let system = System::new("report_routing_service_consumed_message_is_received");
        let subject_addr: Addr<Accountant> = subject.start();
        subject_addr
            .try_send(BindMessage {
                peer_actors: peer_actors_builder().build(),
            })
            .unwrap();

        let earning_wallet = make_wallet("booga");
        subject_addr
            .try_send(ReportRoutingServiceConsumedMessage {
                earning_wallet: earning_wallet.clone(),
                payload_size: 1234,
                service_rate: 42,
                byte_rate: 24,
            })
            .unwrap();

        System::current().stop_with_code(0);
        system.run();
        let more_money_payable_parameters = more_money_payable_parameters_arc.lock().unwrap();
        assert_eq!(
            more_money_payable_parameters[0],
            (make_wallet("booga"), (1 * 42) + (1234 * 24))
        );
        TestLogHandler::new().exists_log_containing(
            &format!("DEBUG: Accountant: Accruing debt to wallet {} for consuming routing service 1234 bytes", earning_wallet),
        );
    }

    #[test]
    fn report_routing_service_consumed_message_is_received_for_our_consuming_wallet() {
        init_test_logging();
        let consuming_wallet = make_wallet("the consuming wallet");
        let config = bc_from_ac_plus_wallets(
            AccountantConfig {
                payable_scan_interval: Duration::from_secs(100),
                payment_received_scan_interval: Duration::from_secs(100),
            },
            consuming_wallet.clone(),
            make_wallet("the earning wallet"),
        );
        let more_money_payable_parameters_arc = Arc::new(Mutex::new(vec![]));
        let payable_dao_mock = PayableDaoMock::new()
            .non_pending_payables_result(vec![])
            .more_money_payable_parameters(more_money_payable_parameters_arc.clone());
        let subject = make_accountant(Some(config), Some(payable_dao_mock), None, None, None);
        let system = System::new("report_routing_service_consumed_message_is_received");
        let subject_addr: Addr<Accountant> = subject.start();
        subject_addr
            .try_send(BindMessage {
                peer_actors: peer_actors_builder().build(),
            })
            .unwrap();

        subject_addr
            .try_send(ReportRoutingServiceConsumedMessage {
                earning_wallet: consuming_wallet.clone(),
                payload_size: 1234,
                service_rate: 42,
                byte_rate: 24,
            })
            .unwrap();

        System::current().stop_with_code(0);
        system.run();
        assert!(more_money_payable_parameters_arc.lock().unwrap().is_empty());

        TestLogHandler::new().exists_log_containing(&format!(
            "INFO: Accountant: Not recording service consumed to our wallet {}",
            consuming_wallet,
        ));
    }

    #[test]
    fn report_routing_service_consumed_message_is_received_for_our_earning_wallet() {
        init_test_logging();
        let earning_wallet = make_wallet("the earning wallet");
        let config = bc_from_ac_plus_earning_wallet(
            AccountantConfig {
                payable_scan_interval: Duration::from_secs(100),
                payment_received_scan_interval: Duration::from_secs(100),
            },
            earning_wallet.clone(),
        );
        let more_money_payable_parameters_arc = Arc::new(Mutex::new(vec![]));
        let payable_dao_mock = PayableDaoMock::new()
            .non_pending_payables_result(vec![])
            .more_money_payable_parameters(more_money_payable_parameters_arc.clone());
        let subject = make_accountant(Some(config), Some(payable_dao_mock), None, None, None);
        let system = System::new("report_routing_service_consumed_message_is_received");
        let subject_addr: Addr<Accountant> = subject.start();
        subject_addr
            .try_send(BindMessage {
                peer_actors: peer_actors_builder().build(),
            })
            .unwrap();

        subject_addr
            .try_send(ReportRoutingServiceConsumedMessage {
                earning_wallet: earning_wallet.clone(),
                payload_size: 1234,
                service_rate: 42,
                byte_rate: 24,
            })
            .unwrap();

        System::current().stop_with_code(0);
        system.run();
        assert!(more_money_payable_parameters_arc.lock().unwrap().is_empty());

        TestLogHandler::new().exists_log_containing(&format!(
            "INFO: Accountant: Not recording service consumed to our wallet {}",
            earning_wallet
        ));
    }

    #[test]
    fn report_exit_service_provided_message_is_received() {
        init_test_logging();
        let config = bc_from_ac_plus_earning_wallet(
            AccountantConfig {
                payable_scan_interval: Duration::from_secs(100),
                payment_received_scan_interval: Duration::from_secs(100),
            },
            make_wallet("hi"),
        );
        let more_money_receivable_parameters_arc = Arc::new(Mutex::new(vec![]));
        let payable_dao_mock = PayableDaoMock::new().non_pending_payables_result(vec![]);
        let receivable_dao_mock = ReceivableDaoMock::new()
            .more_money_receivable_parameters(&more_money_receivable_parameters_arc)
            .more_money_receivable_result(Ok(()));
        let subject = make_accountant(
            Some(config),
            Some(payable_dao_mock),
            Some(receivable_dao_mock),
            None,
            None,
        );
        let system = System::new("report_exit_service_provided_message_is_received");
        let subject_addr: Addr<Accountant> = subject.start();
        subject_addr
            .try_send(BindMessage {
                peer_actors: peer_actors_builder().build(),
            })
            .unwrap();

        let paying_wallet = make_wallet("booga");
        subject_addr
            .try_send(ReportExitServiceProvidedMessage {
                paying_wallet: paying_wallet.clone(),
                payload_size: 1234,
                service_rate: 42,
                byte_rate: 24,
            })
            .unwrap();

        System::current().stop();
        system.run();
        let more_money_receivable_parameters = more_money_receivable_parameters_arc.lock().unwrap();
        assert_eq!(
            more_money_receivable_parameters[0],
            (make_wallet("booga"), (1 * 42) + (1234 * 24))
        );
        TestLogHandler::new().exists_log_containing(&format!(
            "DEBUG: Accountant: Charging exit service for 1234 bytes to wallet {}",
            paying_wallet
        ));
    }

    #[test]
    fn report_exit_service_provided_message_is_received_from_our_consuming_wallet() {
        init_test_logging();
        let consuming_wallet = make_wallet("my consuming wallet");
        let config = bc_from_ac_plus_wallets(
            AccountantConfig {
                payable_scan_interval: Duration::from_secs(100),
                payment_received_scan_interval: Duration::from_secs(100),
            },
            consuming_wallet.clone(),
            make_wallet("my earning wallet"),
        );
        let more_money_receivable_parameters_arc = Arc::new(Mutex::new(vec![]));
        let payable_dao_mock = PayableDaoMock::new().non_pending_payables_result(vec![]);
        let receivable_dao_mock = ReceivableDaoMock::new()
            .more_money_receivable_parameters(&more_money_receivable_parameters_arc);
        let subject = make_accountant(
            Some(config),
            Some(payable_dao_mock),
            Some(receivable_dao_mock),
            None,
            None,
        );
        let system = System::new("report_exit_service_provided_message_is_received");
        let subject_addr: Addr<Accountant> = subject.start();
        subject_addr
            .try_send(BindMessage {
                peer_actors: peer_actors_builder().build(),
            })
            .unwrap();

        subject_addr
            .try_send(ReportExitServiceProvidedMessage {
                paying_wallet: consuming_wallet.clone(),
                payload_size: 1234,
                service_rate: 42,
                byte_rate: 24,
            })
            .unwrap();

        System::current().stop();
        system.run();
        assert!(more_money_receivable_parameters_arc
            .lock()
            .unwrap()
            .is_empty());

        TestLogHandler::new().exists_log_containing(&format!(
            "INFO: Accountant: Not recording service provided for our wallet {}",
            consuming_wallet
        ));
    }

    #[test]
    fn report_exit_service_provided_message_is_received_from_our_earning_wallet() {
        init_test_logging();
        let earning_wallet = make_wallet("my earning wallet");
        let config = bc_from_ac_plus_earning_wallet(
            AccountantConfig {
                payable_scan_interval: Duration::from_secs(100),
                payment_received_scan_interval: Duration::from_secs(100),
            },
            earning_wallet.clone(),
        );
        let more_money_receivable_parameters_arc = Arc::new(Mutex::new(vec![]));
        let payable_dao_mock = PayableDaoMock::new().non_pending_payables_result(vec![]);
        let receivable_dao_mock = ReceivableDaoMock::new()
            .more_money_receivable_parameters(&more_money_receivable_parameters_arc);
        let subject = make_accountant(
            Some(config),
            Some(payable_dao_mock),
            Some(receivable_dao_mock),
            None,
            None,
        );
        let system = System::new("report_exit_service_provided_message_is_received");
        let subject_addr: Addr<Accountant> = subject.start();
        subject_addr
            .try_send(BindMessage {
                peer_actors: peer_actors_builder().build(),
            })
            .unwrap();

        subject_addr
            .try_send(ReportExitServiceProvidedMessage {
                paying_wallet: earning_wallet.clone(),
                payload_size: 1234,
                service_rate: 42,
                byte_rate: 24,
            })
            .unwrap();

        System::current().stop();
        system.run();
        assert!(more_money_receivable_parameters_arc
            .lock()
            .unwrap()
            .is_empty());

        TestLogHandler::new().exists_log_containing(&format!(
            "INFO: Accountant: Not recording service provided for our wallet {}",
            earning_wallet,
        ));
    }

    #[test]
    fn report_exit_service_consumed_message_is_received() {
        init_test_logging();
        let config = bc_from_ac_plus_earning_wallet(
            AccountantConfig {
                payable_scan_interval: Duration::from_secs(100),
                payment_received_scan_interval: Duration::from_secs(100),
            },
            make_wallet("hi"),
        );
        let more_money_payable_parameters_arc = Arc::new(Mutex::new(vec![]));
        let payable_dao_mock = PayableDaoMock::new()
            .non_pending_payables_result(vec![])
            .more_money_payable_parameters(more_money_payable_parameters_arc.clone())
            .more_money_payable_result(Ok(()));
        let subject = make_accountant(Some(config), Some(payable_dao_mock), None, None, None);
        let system = System::new("report_exit_service_consumed_message_is_received");
        let subject_addr: Addr<Accountant> = subject.start();
        subject_addr
            .try_send(BindMessage {
                peer_actors: peer_actors_builder().build(),
            })
            .unwrap();

        let earning_wallet = make_wallet("booga");
        subject_addr
            .try_send(ReportExitServiceConsumedMessage {
                earning_wallet: earning_wallet.clone(),
                payload_size: 1234,
                service_rate: 42,
                byte_rate: 24,
            })
            .unwrap();

        System::current().stop_with_code(0);
        system.run();
        let more_money_payable_parameters = more_money_payable_parameters_arc.lock().unwrap();
        assert_eq!(
            more_money_payable_parameters[0],
            (make_wallet("booga"), (1 * 42) + (1234 * 24))
        );
        TestLogHandler::new().exists_log_containing(&format!(
            "DEBUG: Accountant: Accruing debt to wallet {} for consuming exit service 1234 bytes",
            earning_wallet
        ));
    }

    #[test]
    fn report_exit_service_consumed_message_is_received_for_our_consuming_wallet() {
        init_test_logging();
        let consuming_wallet = make_wallet("own consuming wallet");
        let config = bc_from_ac_plus_wallets(
            AccountantConfig {
                payable_scan_interval: Duration::from_secs(100),
                payment_received_scan_interval: Duration::from_secs(100),
            },
            consuming_wallet.clone(),
            make_wallet("own earning wallet"),
        );
        let more_money_payable_parameters_arc = Arc::new(Mutex::new(vec![]));
        let payable_dao_mock = PayableDaoMock::new()
            .non_pending_payables_result(vec![])
            .more_money_payable_parameters(more_money_payable_parameters_arc.clone());
        let subject = make_accountant(Some(config), Some(payable_dao_mock), None, None, None);
        let system = System::new("report_exit_service_consumed_message_is_received");
        let subject_addr: Addr<Accountant> = subject.start();
        subject_addr
            .try_send(BindMessage {
                peer_actors: peer_actors_builder().build(),
            })
            .unwrap();

        subject_addr
            .try_send(ReportExitServiceConsumedMessage {
                earning_wallet: consuming_wallet.clone(),
                payload_size: 1234,
                service_rate: 42,
                byte_rate: 24,
            })
            .unwrap();

        System::current().stop_with_code(0);
        system.run();
        assert!(more_money_payable_parameters_arc.lock().unwrap().is_empty());

        TestLogHandler::new().exists_log_containing(&format!(
            "INFO: Accountant: Not recording service consumed to our wallet {}",
            consuming_wallet
        ));
    }

    #[test]
    fn report_exit_service_consumed_message_is_received_for_our_earning_wallet() {
        init_test_logging();
        let earning_wallet = make_wallet("own earning wallet");
        let config = bc_from_ac_plus_earning_wallet(
            AccountantConfig {
                payable_scan_interval: Duration::from_secs(100),
                payment_received_scan_interval: Duration::from_secs(100),
            },
            earning_wallet.clone(),
        );
        let more_money_payable_parameters_arc = Arc::new(Mutex::new(vec![]));
        let payable_dao_mock = PayableDaoMock::new()
            .non_pending_payables_result(vec![])
            .more_money_payable_parameters(more_money_payable_parameters_arc.clone());
        let subject = make_accountant(Some(config), Some(payable_dao_mock), None, None, None);
        let system = System::new("report_exit_service_consumed_message_is_received");
        let subject_addr: Addr<Accountant> = subject.start();
        subject_addr
            .try_send(BindMessage {
                peer_actors: peer_actors_builder().build(),
            })
            .unwrap();

        subject_addr
            .try_send(ReportExitServiceConsumedMessage {
                earning_wallet: earning_wallet.clone(),
                payload_size: 1234,
                service_rate: 42,
                byte_rate: 24,
            })
            .unwrap();

        System::current().stop_with_code(0);
        system.run();
        assert!(more_money_payable_parameters_arc.lock().unwrap().is_empty());

        TestLogHandler::new().exists_log_containing(&format!(
            "INFO: Accountant: Not recording service consumed to our wallet {}",
            earning_wallet
        ));
    }

    #[test]
    fn record_service_provided_handles_overflow() {
        init_test_logging();
        let wallet = make_wallet("booga");
        let subject = make_accountant(
            None,
            None,
            Some(ReceivableDaoMock::new().more_money_receivable_result(Err(
                AccountantError::RecordServiceDataOverflow(SignConversionError::U128(
                    "overflow".to_string(),
                )),
            ))),
            None,
            None,
        );

        subject.record_service_provided(i64::MAX as u64, 1, 2, &wallet);

        TestLogHandler::new().exists_log_containing(&format!(
            "ERROR: Accountant: Overflow error trying to record service provided to Node with consuming wallet {}: service rate {}, byte rate 1, payload size 2. Skipping",
            wallet,
            i128::MAX as u128 //TODO how does this condition print into the test?
        ));
    }

    #[test]
    fn record_service_consumed_handles_overflow() {
        init_test_logging();
        let wallet = make_wallet("booga");
        let subject = make_accountant(
            None,
            Some(PayableDaoMock::new().more_money_payable_result(Err(
                AccountantError::RecordServiceDataOverflow(SignConversionError::U64(
                    "overflow".to_string(),
                )),
            ))),
            None,
            None,
            None,
        );

        subject.record_service_consumed(i64::MAX as u64, 1, 2, &wallet);

        TestLogHandler::new().exists_log_containing(&format!(
            "ERROR: Accountant: Overflow error trying to record service consumed from Node with earning wallet {}: service rate {}, byte rate 1, payload size 2. Skipping",
            wallet,
            std::i64::MAX as u64
        ));
    }

    #[test]
    fn handle_sent_payments_handles_overflow() {
        init_test_logging();
        let wallet = make_wallet("booga");
        let payments = SentPayments {
            payments: vec![Ok(Payment::new(
                wallet.clone(),
                u128::MAX,
                H256::from_uint(&U256::from(1)),
            ))],
        };
        let mut subject = make_accountant(
            None,
            Some(
                PayableDaoMock::new().payment_sent_result(Err(AccountantError::PayableError(
                    PayableError::Overflow(SignConversionError::U128("owerflow".to_string())),
                ))),
            ),
            None,
            None,
            None,
        );

        subject.handle_sent_payments(payments);

        TestLogHandler::new().exists_log_containing(
            "ERROR: Accountant: Error from payable: Overflow(U128(\"owerflow\")); amount 340282366920938463463374607431768211455 to address 0x000000000000000000000000000000626f6f6761 on transaction 0x0000…0001"
        );
    }

    #[test]
    fn investigate_debt_extremes_picks_the_most_relevant_records() {
        let now = to_time_t(SystemTime::now());
        let same_amount_significance = 2_000_000;
        let same_age_significance = from_time_t(now - 30000);
        let payables = &[
            PayableAccount {
                wallet: make_wallet("wallet0"),
                balance: same_amount_significance,
                last_paid_timestamp: from_time_t(now - 5000),
                pending_payment_transaction: None,
            },
            //this debt is more significant because beside being high in amount it's also older, so should be prioritized and picked
            PayableAccount {
                wallet: make_wallet("wallet1"),
                balance: same_amount_significance,
                last_paid_timestamp: from_time_t(now - 10000),
                pending_payment_transaction: None,
            },
            //similarly these two wallets have debts equally old but the second has a bigger balance and should be chosen
            PayableAccount {
                wallet: make_wallet("wallet3"),
                balance: 100,
                last_paid_timestamp: same_age_significance,
                pending_payment_transaction: None,
            },
            PayableAccount {
                wallet: make_wallet("wallet2"),
                balance: 330,
                last_paid_timestamp: same_age_significance,
                pending_payment_transaction: None,
            },
        ];

        let result = Accountant::investigate_debt_extremes(payables);

        assert_eq!(result,"Payable scan found 4 debts; the biggest is 2000000 owed for 10000sec, the oldest is 330 owed for 30000sec")
    }

    #[test]
    fn payment_debug_summary_prints_a_nice_summary() {
        let now = to_time_t(SystemTime::now());
        let qualified_payables = &[
            PayableAccount {
                wallet: make_wallet("wallet0"),
                balance: u128_to_signed(PAYMENT_CURVES.permanent_debt_allowed_wei + 1000).unwrap(),
                last_paid_timestamp: from_time_t(
                    now - u64_to_signed(PAYMENT_CURVES.balance_decreases_for_sec).unwrap() - 1234,
                ),
                pending_payment_transaction: None,
            },
            PayableAccount {
                wallet: make_wallet("wallet1"),
                balance: u128_to_signed(PAYMENT_CURVES.permanent_debt_allowed_wei + 1).unwrap(),
                last_paid_timestamp: from_time_t(
                    now - u64_to_signed(PAYMENT_CURVES.balance_decreases_for_sec).unwrap() - 1,
                ),
                pending_payment_transaction: None,
            },
        ];

        let result = Accountant::payments_debug_summary(qualified_payables);

        assert_eq!(result,
                   "Paying qualified debts:\n\
                   10001000 owed for 2593234sec exceeds threshold: 9512428; creditor: 0x0000000000000000000000000077616c6c657430\n\
                   10000001 owed for 2592001sec exceeds threshold: 9999604; creditor: 0x0000000000000000000000000077616c6c657431"
        )
    }

    #[test]
    fn u128_to_signed_gives_a_verbose_error_at_overflow() {
        let result = u128_to_signed(u128::MAX);

        assert_eq!(result,Err(SignConversionError::U128("conversion of 340282366920938463463374607431768211455 from u128 to i128 failed on: out of range integral type conversion attempted".to_string())))
    }

    #[test]
    fn u64_to_signed_gives_a_verbose_error_at_overflow() {
        let result = u64_to_signed(u64::MAX);

        assert_eq!(result,Err(SignConversionError::U64("conversion of 18446744073709551615 from u64 to i64 failed on: out of range integral type conversion attempted".to_string())))
    }

    #[test]
    fn i128_to_unsigned_gives_a_verbose_error_at_overflow() {
        let result = i128_to_unsigned(i128::MIN);

        assert_eq!(result,Err(SignConversionError::I128("conversion of -170141183460469231731687303715884105728 from i128 to u128 failed on: out of range integral type conversion attempted".to_string())))
    }

    #[test]
    fn to_wei_in_i128_provides_correct_values() {
        let result = to_wei_in_i128(55);

        assert_eq!(result, 55_000_000_000)
    }

    fn bc_from_ac_plus_earning_wallet(
        ac: AccountantConfig,
        earning_wallet: Wallet,
    ) -> BootstrapperConfig {
        let mut bc = BootstrapperConfig::new();
        bc.accountant_config = ac;
        bc.earning_wallet = earning_wallet;
        bc
    }

    fn bc_from_ac_plus_wallets(
        ac: AccountantConfig,
        consuming_wallet: Wallet,
        earning_wallet: Wallet,
    ) -> BootstrapperConfig {
        let mut bc = BootstrapperConfig::new();
        bc.accountant_config = ac;
        bc.consuming_wallet = Some(consuming_wallet);
        bc.earning_wallet = earning_wallet;
        bc
    }
}
