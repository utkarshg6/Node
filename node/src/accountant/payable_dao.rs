// Copyright (c) 2019, MASQ (https://masq.ai) and/or its affiliates. All rights reserved.

use crate::accountant::{
    u128_to_signed, u64_to_signed, AccountantError, PayableError, SignConversionError,
};
use std::borrow::Borrow;
use crate::database::connection_wrapper::ConnectionWrapper;
use crate::database::dao_utils;
use crate::database::dao_utils::DaoFactoryReal;
use crate::sub_lib::wallet::Wallet;
use masq_lib::utils::WrapResult;
use rusqlite::types::{ToSql, Type};
use rusqlite::{named_params, Error, OptionalExtension};
use serde_json::{self, json};
use std::fmt::Debug;
use std::time::SystemTime;
use futures::Stream;
use itertools::all;
use web3::types::H256;
use crate::accountant::dao_shared_methods::{insert_or_update_payable, insert_or_update_payable_after_our_payment, InsertUpdateCoreReal, reverse_sign};

#[cfg(test)]
use core::any::Any;

#[derive(Clone, Debug, PartialEq)]
pub struct PayableAccount {
    pub wallet: Wallet,
    pub balance: i128,
    pub last_paid_timestamp: SystemTime,
    pub pending_payment_transaction: Option<H256>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Payment {
    pub to: Wallet,
    pub amount: u128,
    pub timestamp: SystemTime,
    pub transaction: H256,
}

impl Payment {
    pub fn new(to: Wallet, amount: u128, txn: H256) -> Self {
        Self {
            to,
            amount,
            timestamp: SystemTime::now(),
            transaction: txn,
        }
    }
}

pub trait PayableDao: Debug + Send {
    fn more_money_payable(&self, wallet: &Wallet, amount: u128) -> Result<(), AccountantError>;

    fn payment_sent(&self, sent_payment: &Payment) -> Result<(), AccountantError>;

    fn payment_confirmed(
        &self,
        wallet: &Wallet,
        amount: u128,
        confirmation_noticed_timestamp: SystemTime,
        transaction_hash: H256,
    ) -> Result<(), AccountantError>;

    fn account_status(&self, wallet: &Wallet) -> Option<PayableAccount>;

    fn non_pending_payables(&self) -> Vec<PayableAccount>;

    fn top_records(
        &self,
        minimum_amount: i128,
        maximum_age: u64,
    ) -> Result<Vec<PayableAccount>, AccountantError>;

    fn total(&self, inner: &dyn TotalInnerEncapsulationPayable) -> Result<i128,AccountantError>;

    as_any_dcl!();
}

pub trait PayableDaoFactory {
    fn make(&self) -> Box<dyn PayableDao>;
}

impl PayableDaoFactory for DaoFactoryReal {
    fn make(&self) -> Box<dyn PayableDao> {
        Box::new(PayableDaoReal::new(self.make_connection()))
    }
}

#[derive(Debug)]
pub struct PayableDaoReal {
    conn: Box<dyn ConnectionWrapper>,
}

impl PayableDao for PayableDaoReal {
    //TODO we maybe can get rid of the impl of ToSql for Wallet
    fn more_money_payable(&self, wallet: &Wallet, amount: u128) -> Result<(), AccountantError> {
        let amount_signed = u128_to_signed(amount).map_err(|e|e.into_payable().extend("on more money payable"))?;
        match insert_or_update_payable(self.conn.as_ref(),&InsertUpdateCoreReal,&wallet.to_string(),amount_signed)
        {
            Ok(_) => Ok(()),
            Err(e) => unimplemented!(),
        }
    }

    fn payment_sent(&self, payment: &Payment) -> Result<(), AccountantError> {
        let amount_signed = u128_to_signed(payment.amount).map_err(|e|e.into_payable().extend("on payment sent"))?;
        let reversed =reverse_sign(amount_signed).expect("should be within correct range after the previous operation");
        match insert_or_update_payable_after_our_payment(
            self.conn.as_ref(),
            &InsertUpdateCoreReal,
            &payment.to.to_string(),
            reversed,
            dao_utils::to_time_t(payment.timestamp),
            &format!("{:#x}", payment.transaction)
        ) {
            Ok(_) => Ok(()),
            Err(e) => unimplemented!(),
        }
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
        let mut stmt = self.conn
            .prepare("select balance, last_paid_timestamp, pending_payment_transaction from payable where wallet_address = ?")
            .expect("Internal error");
        match stmt
            .query_row(&[&wallet], |row| {
                let balance_result = row.get(0);
                let last_paid_timestamp_result = row.get(1);
                let pending_payment_transaction_result: Result<Option<String>, Error> = row.get(2);
                match (
                    balance_result,
                    last_paid_timestamp_result,
                    pending_payment_transaction_result,
                ) {
                    (Ok(balance), Ok(last_paid_timestamp), Ok(pending_payment_transaction)) => {
                        Ok(PayableAccount {
                            wallet: wallet.clone(),
                            balance,
                            last_paid_timestamp: dao_utils::from_time_t(last_paid_timestamp),
                            pending_payment_transaction: match pending_payment_transaction {
                                Some(tx) => match serde_json::from_value(json!(tx)) {
                                    Ok(transaction) => Some(transaction),
                                    Err(e) => panic!("{:?}", e),
                                },
                                None => None,
                            },
                        })
                    }
                    (r, a, e) => panic!(
                        "Database is corrupt: PAYABLE table columns and/or types {:?}{:?}{:?}",
                        r, a, e
                    ),
                }
            })
            .optional()
        {
            Ok(value) => value,
            Err(e) => panic!("Database is corrupt: {:?}", e),
        }
    }

    fn non_pending_payables(&self) -> Vec<PayableAccount> {
        let mut stmt = self.conn
            .prepare("select balance, last_paid_timestamp, wallet_address from payable where pending_payment_transaction is null")
            .expect("Internal error");

        stmt.query_map([], |row| {
            let balance_result = row.get(0);
            let last_paid_timestamp_result = row.get(1);
            let wallet_result: Result<Wallet, rusqlite::Error> = row.get(2);
            match (balance_result, last_paid_timestamp_result, wallet_result) {
                (Ok(balance), Ok(last_paid_timestamp), Ok(wallet)) => Ok(PayableAccount {
                    wallet,
                    balance,
                    last_paid_timestamp: dao_utils::from_time_t(last_paid_timestamp),
                    pending_payment_transaction: None,
                }),
                _ => panic!("Database is corrupt: PAYABLE table columns and/or types"),
            }
        })
        .expect("Database is corrupt")
        .flatten()
        .collect()
    }

    fn top_records(
        &self,
        minimum_amount: i128,
        maximum_age: u64,
    ) -> Result<Vec<PayableAccount>, AccountantError> {
        let max_age = u64_to_signed(maximum_age)
            .map_err(|e| AccountantError::PayableError(PayableError::Owerflow(e)))?;
        let min_timestamp = dao_utils::now_time_t() - max_age;
        let mut stmt = self
            .conn
            .prepare(
                r#"
                select
                    balance,
                    last_paid_timestamp,
                    wallet_address,
                    pending_payment_transaction
                from
                    payable
                where
                    balance >= ? and
                    last_paid_timestamp >= ?
                order by
                    balance desc,
                    last_paid_timestamp desc
            "#,
            )
            .expect("Internal error");
        let params: &[&dyn ToSql] = &[&minimum_amount, &min_timestamp];
        stmt.query_map(params, |row| {
            let balance_result = row.get(0);
            let last_paid_timestamp_result = row.get(1);
            let wallet_result: Result<Wallet, rusqlite::Error> = row.get(2);
            let pending_payment_transaction_result: Result<Option<String>, Error> = row.get(3);
            match (
                balance_result,
                last_paid_timestamp_result,
                wallet_result,
                pending_payment_transaction_result,
            ) {
                (
                    Ok(balance),
                    Ok(last_paid_timestamp),
                    Ok(wallet),
                    Ok(pending_payment_transaction),
                ) => Ok(PayableAccount {
                    wallet,
                    balance,
                    last_paid_timestamp: dao_utils::from_time_t(last_paid_timestamp),
                    pending_payment_transaction: match pending_payment_transaction {
                        Some(tx) => match serde_json::from_value(json!(tx)) {
                            Ok(transaction) => Some(transaction),
                            Err(e) => panic!("{:?}", e),
                        },
                        None => None,
                    },
                }),
                _ => panic!("Database is corrupt: PAYABLE table columns and/or types"),
            }
        })
        .expect("Database is corrupt")
        .flatten()
        .collect::<Vec<PayableAccount>>()
        .wrap_to_ok()
    }

    fn total(&self, inner:&dyn TotalInnerEncapsulationPayable) -> Result<i128,AccountantError> {
        inner.total_inner(self)
    }

    as_any_impl!();
}

impl PayableDaoReal {
    pub fn new(conn: Box<dyn ConnectionWrapper>) -> PayableDaoReal {
        PayableDaoReal { conn }
    }

    // fn try_increase_balance(&self, wallet: &Wallet, amount: u128) -> Result<bool, AccountantError> {
    //     let mut stmt = self
    //         .conn
    //         .prepare("insert into payable (wallet_address, balance, last_paid_timestamp, pending_payment_transaction) values (:address, :balance, strftime('%s','now'), null) on conflict (wallet_address) do update set balance = balance + :balance where wallet_address = :address")
    //         .expect("Internal error");
    //     let amount = u128_to_signed(amount).map_err(|e| e.into_payable())?;
    //     let params = named_params! {":address": &wallet/*, ":balance": &blob_i128(amount)*/};
    //     match stmt.execute(params) {
    //         Ok(0) => Ok(false),
    //         Ok(_) => Ok(true),
    //         Err(e) => unimplemented!(), //Err(AccountantError::PayableError(PayableError::StatementExecution(format!("increasing balance: {}", e)))),
    //     }
    // }
    //
    // fn try_decrease_balance(
    //     &self,
    //     wallet: &Wallet,
    //     amount: u128,
    //     last_paid_timestamp: SystemTime,
    //     transaction_hash: H256,
    // ) -> Result<bool, AccountantError> {
    //     let mut stmt = self
    //         .conn
    //         .prepare("insert into payable (balance, last_paid_timestamp, pending_payment_transaction, wallet_address) values (:balance, :last_paid, :transaction, :address) on conflict (wallet_address) do update set balance = balance - :balance, last_paid_timestamp = :last_paid, pending_payment_transaction = :transaction where wallet_address = :address")
    //         .expect("Internal error");
    //     let amount = u128_to_signed(amount).map_err(|e| e.into_payable())?;
    //     let params = named_params! {
    //         /*":balance": &blob_i128(amount),*/
    //         ":last_paid": &dao_utils::to_time_t(last_paid_timestamp),
    //         ":transaction": &format!("{:#x}", &transaction_hash),
    //         ":address": &wallet
    //     };
    //     match stmt.execute(params) {
    //         Ok(0) => Ok(false),
    //         Ok(_) => Ok(true),
    //         Err(e) => unimplemented!(), // Err(format!("{}", e)),
    //     }
    // }
}

pub trait TotalInnerEncapsulationPayable{
    fn total_inner(&self,dao:&dyn PayableDao)->Result<i128,AccountantError>;
}

pub struct TotalInnerEncapsulationPayableReal;

impl TotalInnerEncapsulationPayable for TotalInnerEncapsulationPayableReal{
    fn total_inner(&self, dao: &dyn PayableDao) -> Result<i128, AccountantError> {
        let all_records = match dao.top_records(i128::MIN,u64::MAX/2){
            Ok(records) => records,
            Err(e) => return Err(e.extend("on calling total()"))
        };
        let init = 0_i128;
        let total = all_records.into_iter().fold(init,|so_far,current_one|{
            so_far + current_one.balance
        });
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::ptr::addr_of;
    use super::*;
    use crate::database::dao_utils::from_time_t;
    use crate::database::db_initializer;
    use crate::database::db_initializer::{DbInitializer, DbInitializerReal};
    use crate::test_utils::make_wallet;
    use ethereum_types::BigEndianHash;
    use masq_lib::test_utils::utils::{ensure_node_home_directory_exists, TEST_DEFAULT_CHAIN};
    use rusqlite::{Connection, OpenFlags};
    use std::str::FromStr;
    use std::sync::{Arc, Mutex};
    use web3::types::U256;
    use crate::accountant::test_utils::PayableDaoMock;
    use crate::database::connection_wrapper::ConnectionWrapperReal;
    use crate::database::db_initializer::test_utils::ConnectionWrapperMock;

    #[test]
    fn more_money_payable_works_for_new_address() {
        let home_dir = ensure_node_home_directory_exists(
            "payable_dao",
            "more_money_payable_works_for_new_address",
        );
        let before = dao_utils::to_time_t(SystemTime::now());
        let wallet = make_wallet("booga");
        let status = {
            let subject = PayableDaoReal::new(
                DbInitializerReal::default()
                    .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
                    .unwrap(),
            );

            subject.more_money_payable(&wallet, 1234).unwrap();
            subject.account_status(&wallet).unwrap()
        };

        let after = dao_utils::to_time_t(SystemTime::now());
        assert_eq!(status.wallet, wallet);
        assert_eq!(status.balance, 1234);
        let timestamp = dao_utils::to_time_t(status.last_paid_timestamp);
        assert!(
            timestamp >= before,
            "{:?} should be on or after {:?}",
            timestamp,
            before
        );
        assert!(
            timestamp <= after,
            "{:?} should be on or before {:?}",
            timestamp,
            after
        );
    }

    #[test]
    fn more_money_payable_works_for_existing_address() {
        let home_dir = ensure_node_home_directory_exists(
            "payable_dao",
            "more_money_payable_works_for_existing_address",
        );
        let wallet = make_wallet("booga");
        let subject = {
            let subject = PayableDaoReal::new(
                DbInitializerReal::default()
                    .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
                    .unwrap(),
            );
            subject.more_money_payable(&wallet, 1234).unwrap();
            let mut flags = OpenFlags::empty();
            flags.insert(OpenFlags::SQLITE_OPEN_READ_WRITE);
            let conn =
                Connection::open_with_flags(&home_dir.join(db_initializer::DATABASE_FILE), flags)
                    .unwrap();
            conn.execute(
                "update payable set last_paid_timestamp = 0 where wallet_address = '0x000000000000000000000000000000626f6f6761'",
                [],
            )
            .unwrap();
            subject
        };

        let status = {
            subject.more_money_payable(&wallet, 2345).unwrap();
            subject.account_status(&wallet).unwrap()
        };

        assert_eq!(status.wallet, wallet);
        assert_eq!(status.balance, 3579);
        assert_eq!(status.last_paid_timestamp, SystemTime::UNIX_EPOCH);
    }

    #[test]
    fn more_money_payable_works_for_overflow() {
        let home_dir = ensure_node_home_directory_exists(
            "payable_dao",
            "more_money_payable_works_for_overflow",
        );
        let wallet = make_wallet("booga");
        let subject = PayableDaoReal::new(
            DbInitializerReal::default()
                .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
                .unwrap(),
        );

        let result = subject.more_money_payable(&wallet, std::u128::MAX);

        assert_eq!(
            result,
            Err(AccountantError::PayableError(PayableError::Owerflow(
                SignConversionError::U128("conversion of 340282366920938463463374607431768211455 \
                 from u128 to i128 failed on: out of range integral type conversion attempted; on more money payable".to_string())
            )))
        );
    }

    #[test]
    fn payment_sent_records_a_pending_transaction_for_a_new_address() {
        let home_dir = ensure_node_home_directory_exists(
            "payable_dao",
            "payment_sent_records_a_pending_transaction_for_a_new_address",
        );
        let wallet = make_wallet("booga");
        let subject = PayableDaoReal::new(
            DbInitializerReal::default()
                .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
                .unwrap(),
        );
        let payment = Payment::new(wallet.clone(), 1, H256::from_uint(&U256::from(1)));

        let before_account_status = subject.account_status(&payment.to);
        assert!(before_account_status.is_none());

        subject.payment_sent(&payment).unwrap();

        let after_account_status = subject.account_status(&payment.to).unwrap();

        assert_eq!(
            after_account_status.clone(),
            PayableAccount {
                wallet,
                balance: -1,
                last_paid_timestamp: after_account_status.last_paid_timestamp,
                pending_payment_transaction: Some(H256::from_uint(&U256::from(1))),
            }
        )
    }

    #[test]
    fn payment_sent_records_a_pending_transaction_for_an_existing_address() {
        let home_dir = ensure_node_home_directory_exists(
            "payable_dao",
            "payment_sent_records_a_pending_transaction_for_an_existing_address",
        );
        let wallet = make_wallet("booga");
        let subject = PayableDaoReal::new(
            DbInitializerReal::default()
                .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
                .unwrap(),
        );
        let payment = Payment::new(wallet.clone(), 1, H256::from_uint(&U256::from(1)));

        let before_account_status = subject.account_status(&payment.to);
        assert!(before_account_status.is_none());
        subject.more_money_payable(&wallet, 1).unwrap();
        subject.payment_sent(&payment).unwrap();

        let after_account_status = subject.account_status(&payment.to).unwrap();

        assert_eq!(
            after_account_status.clone(),
            PayableAccount {
                wallet,
                balance: 0,
                last_paid_timestamp: after_account_status.last_paid_timestamp,
                pending_payment_transaction: Some(H256::from_uint(&U256::from(1))),
            }
        )
    }

    #[test]
    fn payment_sent_works_for_overflow() {
        let home_dir =
            ensure_node_home_directory_exists("payable_dao", "payment_sent_works_for_overflow");
        let wallet = make_wallet("booga");
        let subject = PayableDaoReal::new(
            DbInitializerReal::default()
                .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
                .unwrap(),
        );
        let payment = Payment::new(wallet, u128::MAX, H256::from_uint(&U256::from(1)));

        let result = subject.payment_sent(&payment);

        assert_eq!(
            result,
            Err(AccountantError::PayableError(PayableError::Owerflow(
                SignConversionError::U128("blah".to_string())
            )))
        )
    }

    #[test]
    fn payment_confirmed_works_for_overflow() {
        let home_dir = ensure_node_home_directory_exists(
            "payable_dao",
            "payment_confirmed_works_for_overflow",
        );
        let wallet = make_wallet("booga");
        let subject = PayableDaoReal::new(
            DbInitializerReal::default()
                .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
                .unwrap(),
        );

        let result = subject.payment_confirmed(
            &wallet,
            u128::MAX,
            SystemTime::now(),
            H256::from_uint(&U256::from(1)),
        );

        assert_eq!(
            result,
            Err(AccountantError::PayableError(PayableError::Owerflow(
                SignConversionError::U128("blah".to_string())
            )))
        )
    }

    #[test]
    fn payable_account_status_works_when_account_doesnt_exist() {
        let home_dir = ensure_node_home_directory_exists(
            "payable_dao",
            "payable_account_status_works_when_account_doesnt_exist",
        );
        let wallet = make_wallet("booga");
        let subject = PayableDaoReal::new(
            DbInitializerReal::default()
                .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
                .unwrap(),
        );

        let result = subject.account_status(&wallet);

        assert_eq!(result, None);
    }

    #[test]
    fn non_pending_payables_should_return_an_empty_vec_when_the_database_is_empty() {
        let home_dir = ensure_node_home_directory_exists(
            "payable_dao",
            "non_pending_payables_should_return_an_empty_vec_when_the_database_is_empty",
        );

        let subject = PayableDaoReal::new(
            DbInitializerReal::default()
                .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
                .unwrap(),
        );

        assert_eq!(subject.non_pending_payables(), vec![]);
    }

    #[test]
    fn non_pending_payables_should_return_payables_with_no_pending_transaction() {
        let home_dir = ensure_node_home_directory_exists(
            "payable_dao",
            "non_pending_payables_should_return_payables_with_no_pending_transaction",
        );

        let subject = PayableDaoReal::new(
            DbInitializerReal::default()
                .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
                .unwrap(),
        );

        let mut flags = OpenFlags::empty();
        flags.insert(OpenFlags::SQLITE_OPEN_READ_WRITE);
        let conn =
            Connection::open_with_flags(&home_dir.join(db_initializer::DATABASE_FILE), flags)
                .unwrap();
        let insert = |wallet: &str, balance: i64, pending_payment_transaction: Option<&str>| {
            let params: &[&dyn ToSql] = &[&wallet, &balance, &0i64, &pending_payment_transaction];

            conn
                .prepare("insert into payable (wallet_address, balance, last_paid_timestamp, pending_payment_transaction) values (?, ?, ?, ?)")
                .unwrap()
                .execute(params)
                .unwrap();
        };

        insert(
            "0x0000000000000000000000000000000000666f6f",
            42,
            Some("0x155553215215"),
        );
        insert(
            "0x0000000000000000000000000000000000626172",
            24,
            Some("0x689477777623"),
        );
        insert("0x0000000000000000000000000000666f6f626172", 44, None);
        insert("0x0000000000000000000000000000626172666f6f", 22, None);

        let result = subject.non_pending_payables();

        assert_eq!(
            result,
            vec![
                PayableAccount {
                    wallet: make_wallet("foobar"),
                    balance: 44,
                    last_paid_timestamp: from_time_t(0),
                    pending_payment_transaction: None
                },
                PayableAccount {
                    wallet: make_wallet("barfoo"),
                    balance: 22,
                    last_paid_timestamp: from_time_t(0),
                    pending_payment_transaction: None
                },
            ]
        );
    }

    #[test]
    fn payable_amount_errors_on_insert_when_out_of_range() {
        let home_dir = ensure_node_home_directory_exists(
            "payable_dao",
            "payable_amount_precision_loss_panics_on_insert",
        );
        let subject = PayableDaoReal::new(
            DbInitializerReal::default()
                .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
                .unwrap(),
        );

        let result = subject.more_money_payable(&make_wallet("foobar"), u128::MAX);

        assert_eq!(
            result,
            Err(AccountantError::PayableError(PayableError::Owerflow(
                SignConversionError::U128("conversion of 340282366920938463463374607431768211455 \
                 from u128 to i128 failed on: out of range integral type conversion attempted; on more money payable".to_string())
            )))
        )
    }

    #[test]
    fn payable_amount_errors_on_update_balance_when_out_of_range() {
        let home_dir = ensure_node_home_directory_exists(
            "payable_dao",
            "payable_amount_precision_loss_panics_on_update_balance",
        );
        let subject = PayableDaoReal::new(
            DbInitializerReal::default()
                .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
                .unwrap(),
        );

        let result = subject.payment_sent(&Payment::new(
            make_wallet("foobar"),
            u128::MAX,
            H256::from_uint(&U256::from(123)),
        ));

        assert_eq!(
            result,
            Err(AccountantError::PayableError(PayableError::Owerflow(
                SignConversionError::U128("conversion of 340282366920938463463374607431768211455 \
                 from u128 to i128 failed on: out of range integral type conversion attempted; on payment sent".to_string())
            )))
        )
    }

    #[test]
    fn top_records_works(){
        let home_dir = ensure_node_home_directory_exists("payable_dao", "top_records_works");
        let conn = DbInitializerReal::default()
            .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
            .unwrap();
        let balance1 = 999_999_999; // below minimum amount - reject
        let balance2 = 1_000_000_000; // minimum amount
        let balance3 = 1_000_000_000; // minimum amount
        let balance4 = 1_000_000_001; // above minimum amount
        let timestamp1 = dao_utils::now_time_t() - 80_000; // below maximum age
        let timestamp2 = dao_utils::now_time_t() - 86_401; // above maximum age - reject
        let timestamp3 = dao_utils::now_time_t() - 86_000; // below maximum age
        let timestamp4 = dao_utils::now_time_t() - 86_001; // below maximum age
        make_multiple_insertions_with_balances_and_timestamps_opt(
            conn.as_ref(),
            balance1,
            balance2,
            balance3,
            balance4,
            Some(timestamp1),
            Some(timestamp2),
            Some(timestamp3),
            Some(timestamp4)
        );
        let subject = PayableDaoReal::new(conn);

        let top_records = subject.top_records(1_000_000_000, 86400).unwrap();

        assert_eq!(
            top_records,
            vec![
                PayableAccount {
                    wallet: Wallet::new("0x4444444444444444444444444444444444444444"),
                    balance: 1_000_000_001,
                    last_paid_timestamp: dao_utils::from_time_t(timestamp4),
                    pending_payment_transaction: Some(
                        H256::from_str(
                            "1111111122222222333333334444444455555555666666667777777788888888"
                        )
                        .unwrap()
                    )
                },
                PayableAccount {
                    wallet: Wallet::new("0x3333333333333333333333333333333333333333"),
                    balance: 1_000_000_000,
                    last_paid_timestamp: dao_utils::from_time_t(timestamp3),
                    pending_payment_transaction: None
                },
            ]
        );
    }

    #[test]
    fn total_works_real(){
        let home_dir = ensure_node_home_directory_exists("payable_dao", "total_works");
        let conn = DbInitializerReal::default()
            .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
            .unwrap();
        let balance1 = 999_999_999;
        let balance2 = 1_000_000_000;
        let balance3 = 1_000_000_000;
        let balance4 = 1_000_000_001;
        make_multiple_insertions_with_balances_and_timestamps_opt(
            conn.as_ref(),
            balance1,
            balance2,
            balance3,
            balance4,
            None,
            None,
            None,
            None
        );
        let subject = PayableDaoReal::new(conn);

        let result = subject.total(&TotalInnerEncapsulationPayableReal).unwrap();

        assert_eq!(result,4_000_000_000)
    }

    #[test]
    fn total_inner_handles_error_producing_list_of_records_to_sum_up_the_balances_for(){
        let top_records_parameters_arc = Arc::new(Mutex::new(vec![]));
        let dao_mock = PayableDaoMock::new()
            .top_records_parameters(&top_records_parameters_arc)
            .top_records_result(Result::Err(AccountantError::PayableError(PayableError::RusqliteError("broken".to_string()))));
        let subject = TotalInnerEncapsulationPayableReal;

        let result = subject.total_inner(&dao_mock);

        assert_eq!(result,Err(AccountantError::PayableError(PayableError::RusqliteError("broken; on calling total()".to_string()))));
        let top_records_parameters = top_records_parameters_arc.lock().unwrap();
        assert_eq!(*top_records_parameters,vec![(i128::MIN,u64::MAX/2)])
    }

    #[test]
    fn total_inner_is_hooked_to_total_public(){
       let prepare_params_arc = Arc::new(Mutex::new(vec![]));
       let fake_conn = ConnectionWrapperMock::default()
           .prepare_params(&prepare_params_arc)
           .prepare_result(Err(rusqlite::Error::BlobSizeError)); //this error is imposed, has nothing to do with the goal of this test
       let subject = PayableDaoReal::new(Box::new(fake_conn));
       let mut inner = TotalInnerEncapsulationPayableMock::default()
           .total_inner_result(Ok(-45678));
       inner.testing_real_dao = true;

       let result = subject.total(&inner);

       assert_eq!(result,Ok(-45678));
       let prepare_params = prepare_params_arc.lock().unwrap();
       assert_eq!(*prepare_params,vec!["Yes, I'm hooked"])
    }

    #[test]
    fn correctly_totals_zero_records() {
        let home_dir =
            ensure_node_home_directory_exists("payable_dao", "correctly_totals_zero_records");
        let conn = DbInitializerReal::default()
            .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
            .unwrap();
        let subject = PayableDaoReal::new(conn);

        let result = subject.total(&TotalInnerEncapsulationPayableReal).unwrap();

        assert_eq!(result, 0)
    }

    fn make_multiple_insertions_with_balances_and_timestamps_opt(
        conn:&dyn ConnectionWrapper,
        balance1:i128,
        balance2:i128,
        balance3:i128,
        balance4:i128,
        timestamp1: Option<i64>,
        timestamp2:Option<i64>,
        timestamp3:Option<i64>,
        timestamp4:Option<i64>)
    {
        insert_into_payable(
            conn,
            "0x1111111111111111111111111111111111111111",
            balance1,
            timestamp1.unwrap_or(0),
            None,
        );
        insert_into_payable(
            conn,
            "0x2222222222222222222222222222222222222222",
            balance2,
            timestamp2.unwrap_or(1),
            None,
        );
        insert_into_payable(
            conn,
            "0x3333333333333333333333333333333333333333",
            balance3,
            timestamp3.unwrap_or(2),
            None,
        );
        insert_into_payable(
            conn,
            "0x4444444444444444444444444444444444444444",
            balance4,
            timestamp4.unwrap_or(3),
            Some("0x1111111122222222333333334444444455555555666666667777777788888888"),
        );
    }

    fn insert_into_payable(
        conn: &dyn ConnectionWrapper,
        wallet: &str,
        balance: i128,
        timestamp: i64,
        pending_payment_transaction: Option<&str>)
    {
        let params: &[&dyn ToSql] =
            &[&wallet, &balance, &timestamp, &pending_payment_transaction];
        conn
            .prepare("insert into payable (wallet_address, balance, last_paid_timestamp, pending_payment_transaction) values (?, ?, ?, ?)")
            .unwrap()
            .execute(params)
            .unwrap();
    }

    #[derive(Default)]
    struct TotalInnerEncapsulationPayableMock{
        testing_real_dao: bool,
        total_inner_results: RefCell<Vec<Result<i128,AccountantError>>>

    }

    impl TotalInnerEncapsulationPayable for TotalInnerEncapsulationPayableMock{
        fn total_inner(&self, dao: &dyn PayableDao) -> Result<i128, AccountantError> {
            if self.testing_real_dao {
                let dao = dao.as_any().downcast_ref::<PayableDaoReal>().unwrap();
                let _ = dao.conn.prepare("Yes, I'm hooked"); //special test; a single use
            }
            self.total_inner_results.borrow_mut().remove(0)
        }
    }

    impl TotalInnerEncapsulationPayableMock{
        fn total_inner_result(self,result: Result<i128,AccountantError>)->Self{
            self.total_inner_results.borrow_mut().push(result);
            self
        }
    }
}
