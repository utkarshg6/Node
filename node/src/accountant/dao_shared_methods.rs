// Copyright (c) 2019, MASQ (https://masq.ai) and/or its affiliates. All rights reserved.

use crate::accountant::receivable_dao::ReceivableError;
use crate::accountant::{AccountantError, PayableError, SignConversionError};
use crate::database::connection_wrapper::ConnectionWrapper;
use rusqlite::types::Value;
use rusqlite::ToSql;
use std::fmt::{Display, Formatter};
use web3::types::Res;
use crate::accountant::payable_dao::PayableDao;

#[derive(Clone)]
pub struct InsertUpdateConfig<'a> {
    insert_sql: &'a str,
    update_sql: &'a str,
    params: &'a [(&'static str, &'a dyn ToSql)],
    table: Table,
}

pub struct UpdateConfig<'a> {
    update_sql: &'a str,
    params: &'a [(&'static str, &'a dyn ToSql)],
    table: Table,
}

#[derive(PartialEq, Debug, Clone)]
enum Table {
    Payable,
    Receivable,
}

impl Display for Table {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Table::Payable => write!(f, "payable"),
            Table::Receivable => write!(f, "receivable"),
        }
    }
}

pub trait InsertUpdateCore {
    fn update(
        &self,
        conn: &dyn ConnectionWrapper,
        wallet: &str,
        amount: i128,
        config: &dyn UpdateConfiguration,
    ) -> Result<(), String>;
    fn insert_or_update<'a>(
        &self,
        conn: &dyn ConnectionWrapper,
        wallet: &str,
        amount: i128,
        config: InsertUpdateConfig<'a>,
    ) -> Result<(), String>;
}

pub struct InsertUpdateCoreReal;

impl InsertUpdateCore for InsertUpdateCoreReal {
    fn update(
        &self,
        conn: &dyn ConnectionWrapper,
        wallet: &str,
        amount: i128,
        config: &dyn UpdateConfiguration,
    ) -> Result<(), String> {
        let mut statement = conn
            .prepare(config.select_sql().as_str())
            .expect("internal rusqlite error");
        match statement.query_row([wallet], |row| {
            let balance_result: rusqlite::Result<i128> = row.get(0);
            match balance_result {
                Ok(balance) => {
                    let updated_balance = balance + amount;
                    let blob = Self::blob_i128(updated_balance);
                    let mut adjusted_params = config.update_params().to_vec();
                    config.should_balance_param_be_removed(&mut adjusted_params);
                    adjusted_params.insert(0, (":updated_balance", &blob));
                    let mut stm = conn
                        .prepare(config.update_sql())
                        .expect("internal rusqlite error");
                    stm.execute(&*adjusted_params)
                }
                Err(e) => Err(e),
            }
        }) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!(
                "Updating balance for {} of {} Wei to {}; failing on: '{}'",
                config.table(),
                amount,
                wallet,
                e
            )),
        }
    }

    fn insert_or_update(
        &self,
        conn: &dyn ConnectionWrapper,
        wallet: &str,
        amount: i128,
        config: InsertUpdateConfig,
    ) -> Result<(), String> {
        let params = config.params;
        let mut stm = conn
            .prepare(config.insert_sql)
            .expect("internal rusqlite error");
        match stm.execute(&*params) {
            Ok(_) => return Ok(()),
            Err(e) => {
                let cause = e.to_string();
                let constraint_failed_msg = "UNIQUE constraint failed";
                if cause.len() >= constraint_failed_msg.len()
                    && &cause[..constraint_failed_msg.len()] == constraint_failed_msg
                {
                    self.update(conn, wallet, amount, &config)
                } else {
                    Err(format!("Updating balance after invalid insertion for {} of {} Wei to {}; failing on: '{}'", config.table, amount, wallet, cause))
                }
            }
        }
    }
}

impl InsertUpdateCoreReal {
    pub fn blob_i128(amount: i128) -> Value {
        amount.into()
    }
}

pub trait UpdateConfiguration<'a> {
    fn table(&self) -> String;
    fn select_sql(&self) -> String;
    fn update_sql(&self) -> &'a str;
    fn update_params(&self) -> &'a [(&'static str, &'a dyn ToSql)];
    fn should_balance_param_be_removed(&self, params_to_adjust: &mut Vec<(&str, &dyn ToSql)>);
}

impl<'a> UpdateConfiguration<'a> for InsertUpdateConfig<'a> {
    fn table(&self) -> String {
        self.table.to_string()
    }

    fn select_sql(&self) -> String {
        format!(
            "select balance from {} where wallet_address = ?",
            self.table
        )
    }

    fn update_sql(&self) -> &'a str {
        self.update_sql
    }

    fn update_params(&self) -> &'a [(&'static str, &'a dyn ToSql)] {
        self.params
    }

    fn should_balance_param_be_removed(&self, params_to_adjust: &mut Vec<(&str, &dyn ToSql)>) {
        let _ = params_to_adjust.remove(1);
    }
}

impl<'a> UpdateConfiguration<'a> for UpdateConfig<'a> {
    fn table(&self) -> String {
        self.table.to_string()
    }

    fn select_sql(&self) -> String {
        format!(
            "select balance from {} where wallet_address = :wallet",
            self.table
        )
    }

    fn update_sql(&self) -> &'a str {
        self.update_sql
    }

    fn update_params(&self) -> &'a [(&'static str, &'a dyn ToSql)] {
        self.params
    }

    fn should_balance_param_be_removed(&self, _params_to_adjust: &mut Vec<(&str, &dyn ToSql)>) {
        ()
    }
}

//update
pub fn update_receivable(
    conn: &dyn ConnectionWrapper,
    core: &dyn InsertUpdateCore,
    wallet: &str,
    amount: i128,
    last_received_time_stamp: i64,
) -> Result<(), AccountantError> {
    let config = UpdateConfig {
        update_sql: "update receivable set balance = :updated_balance, last_received_timestamp = :last_received where wallet_address = :wallet",
        params: &[(":wallet",&wallet),(":last_received",&last_received_time_stamp)],
        table: Table::Receivable
    };
    core.update(conn, wallet, amount, &config)
        .map_err(receivable_rusqlite_error)
}

//insert_update
pub fn insert_or_update_receivable(
    conn: &dyn ConnectionWrapper,
    core: &dyn InsertUpdateCore,
    wallet: &str,
    amount: i128,
) -> Result<(), AccountantError> {
    let config = InsertUpdateConfig{
        insert_sql: "insert into receivable (wallet_address, balance, last_received_timestamp) values (:wallet,:balance,strftime('%s','now'))",
        update_sql: "update receivable set balance = :updated_balance where wallet_address = :wallet",
        params: &[(":wallet", &wallet), (":balance", &amount)],
        table: Table::Receivable
    };
    core.insert_or_update(conn, wallet, amount, config)
        .map_err(receivable_rusqlite_error)
}

pub fn insert_or_update_payable(
    conn: &dyn ConnectionWrapper,
    core: &dyn InsertUpdateCore,
    wallet: &str,
    amount: i128,
) -> Result<(), AccountantError> {
    let config = InsertUpdateConfig{
        insert_sql: "insert into payable (wallet_address, balance, last_paid_timestamp, pending_payment_transaction) values (:wallet,:balance,strftime('%s','now'),null)",
        update_sql: "update payable set balance = :updated_balance where wallet_address = :wallet",
        params: &[(":wallet", &wallet), (":balance", &amount)],
        table: Table::Payable
    };
    core.insert_or_update(conn, wallet, amount, config)
        .map_err(payable_rusqlite_error)
}

pub fn insert_or_update_payable_after_our_payment(
    conn: &dyn ConnectionWrapper,
    core: &dyn InsertUpdateCore,
    wallet: &str,
    amount: i128,
    last_paid_timestamp: i64,
    transaction_hash: &str,
) -> Result<(), AccountantError> {
    let config = InsertUpdateConfig{
        insert_sql: "insert into payable (balance, last_paid_timestamp, pending_payment_transaction, wallet_address) values (:balance, :last_paid, :transaction, :wallet)",
        update_sql: "update payable set balance = :updated_balance, last_paid_timestamp = :last_paid, pending_payment_transaction = :transaction where wallet_address = :wallet",
        params: &[(":wallet", &wallet), (":balance", &amount), (":last_paid", &last_paid_timestamp), (":transaction", &transaction_hash)],
        table: Table::Payable
    };
    core.insert_or_update(conn, wallet, amount, config)
        .map_err(payable_rusqlite_error)
}

fn receivable_rusqlite_error(err: String) -> AccountantError {
    AccountantError::ReceivableError(ReceivableError::RusqliteError(err))
}

fn payable_rusqlite_error(err: String) -> AccountantError {
    AccountantError::PayableError(PayableError::RusqliteError(err))
}

pub fn reverse_sign(amount:i128)->Result<i128,SignConversionError>{
    amount.checked_abs().map(|val|-val).ok_or_else(||SignConversionError::I128(format!("Reversing the sign for value: {}",amount)))
}

#[cfg(test)]
mod tests {
    use crate::accountant::dao_shared_methods::{insert_or_update_payable, insert_or_update_payable_after_our_payment, insert_or_update_receivable, update_receivable, InsertUpdateConfig, InsertUpdateCore, InsertUpdateCoreReal, Table, UpdateConfig, UpdateConfiguration, reverse_sign};
    use crate::accountant::receivable_dao::ReceivableError;
    use crate::accountant::{AccountantError, PayableError, SignConversionError};
    use crate::database::connection_wrapper::{ConnectionWrapper, ConnectionWrapperReal};
    use crate::database::db_initializer::{DbInitializer, DbInitializerReal};
    use masq_lib::blockchains::chains::Chain;
    use masq_lib::test_utils::utils::ensure_node_home_directory_exists;
    use masq_lib::utils::SliceToVec;
    use rusqlite::types::Value;
    use rusqlite::{named_params, params, Connection};
    use std::cell::RefCell;
    use std::ptr::addr_of;
    use std::sync::{Arc, Mutex};
    use std::time::SystemTime;
    #[derive(Default)]
    struct InsertUpdateCoreMock {
        update_params: Arc<Mutex<Vec<(String, i128, (String, String, String, Vec<String>))>>>, //trait-object-like params tested specially
        update_results: RefCell<Vec<Result<(), String>>>,
        insert_or_update_params:
            Arc<Mutex<Vec<(String, i128, (String, String, Table, Vec<String>))>>>, //I have to skip the sql params which cannot be handled in a test economically
        insert_or_update_results: RefCell<Vec<Result<(), String>>>,
        connection_wrapper_as_pointer_to_compare: Option<*const dyn ConnectionWrapper>,
    }

    impl InsertUpdateCore for InsertUpdateCoreMock {
        fn update(
            &self,
            conn: &dyn ConnectionWrapper,
            wallet: &str,
            amount: i128,
            config: &dyn UpdateConfiguration,
        ) -> Result<(), String> {
            let owned_params: Vec<String> = config
                .update_params()
                .into_iter()
                .map(|(str, _to_sql)| str.to_string())
                .collect();
            self.update_params.lock().unwrap().push((
                wallet.to_string(),
                amount,
                (
                    config.select_sql(),
                    config.update_sql().to_string(),
                    config.table(),
                    owned_params,
                ),
            ));
            if let Some(conn_wrapp_pointer) = self.connection_wrapper_as_pointer_to_compare {
                assert_eq!(conn_wrapp_pointer, addr_of!(*conn))
            }
            self.update_results.borrow_mut().remove(0)
        }

        fn insert_or_update(
            &self,
            conn: &dyn ConnectionWrapper,
            wallet: &str,
            amount: i128,
            config: InsertUpdateConfig,
        ) -> Result<(), String> {
            let owned_params: Vec<String> = config
                .params
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
        fn update_params(
            mut self,
            params: &Arc<Mutex<Vec<(String, i128, (String, String, String, Vec<String>))>>>,
        ) -> Self {
            self.update_params = params.clone();
            self
        }

        fn update_result(self, result: Result<(), String>) -> Self {
            self.update_results.borrow_mut().push(result);
            self
        }

        fn insert_or_update_params(
            mut self,
            params: &Arc<Mutex<Vec<(String, i128, (String, String, Table, Vec<String>))>>>,
        ) -> Self {
            self.insert_or_update_params = params.clone();
            self
        }

        fn insert_or_update_results(self, result: Result<(), String>) -> Self {
            self.insert_or_update_results.borrow_mut().push(result);
            self
        }
    }

    #[test]
    fn update_receivable_works_positive() {
        let wallet_address = "xyz123";
        let path = ensure_node_home_directory_exists(
            "dao_shared_methods",
            "update_receivable_works_positive",
        );
        let conn = DbInitializerReal::default()
            .initialize(&path, Chain::PolyMainnet, true)
            .unwrap();
        let conn_ref = conn.as_ref();
        insert_receivable_100_balance_100_last_time_stamp(conn_ref, wallet_address);
        let (balance, last_time_stamp) = read_row_receivable(conn_ref, wallet_address).unwrap();
        assert_eq!(balance, 100);
        assert_eq!(last_time_stamp, 100);
        let amount_wei = i128::MAX - 100;
        let last_received_time_stamp_sec = 4_545_789;

        let result = update_receivable(
            conn_ref,
            &InsertUpdateCoreReal,
            wallet_address,
            amount_wei,
            last_received_time_stamp_sec,
        );

        assert_eq!(result, Ok(()));
        let (balance, last_time_stamp) = read_row_receivable(conn_ref, wallet_address).unwrap();
        assert_eq!(balance, amount_wei + 100);
        assert_eq!(last_time_stamp, last_received_time_stamp_sec);
    }

    #[test]
    fn update_receivable_works_negative() {
        let wallet_address = "xyz178";
        let path = ensure_node_home_directory_exists(
            "dao_shared_methods",
            "update_receivable_works_negative",
        );
        let conn = DbInitializerReal::default()
            .initialize(&path, Chain::PolyMainnet, true)
            .unwrap();
        let conn_ref = conn.as_ref();
        insert_receivable_100_balance_100_last_time_stamp(conn_ref, wallet_address);
        let (balance, last_time_stamp) = read_row_receivable(conn_ref, wallet_address).unwrap();
        assert_eq!(balance, 100);
        assert_eq!(last_time_stamp, 100);
        let amount_wei = -345_125;
        let last_received_time_stamp_sec = 4_545_789;

        let result = update_receivable(
            conn_ref,
            &InsertUpdateCoreReal,
            wallet_address,
            amount_wei,
            last_received_time_stamp_sec,
        );

        assert_eq!(result, Ok(()));
        let (balance, last_time_stamp) = read_row_receivable(conn_ref, wallet_address).unwrap();
        assert_eq!(balance, -345_025);
        assert_eq!(last_time_stamp, last_received_time_stamp_sec);
    }

    #[test]
    fn insert_or_update_receivable_works_for_insert_positive() {
        let wallet_address = "xyz456";
        let path = ensure_node_home_directory_exists(
            "dao_shared_methods",
            "insert_or_update_receivable_works_for_insert_positive",
        );
        let conn = DbInitializerReal::default()
            .initialize(&path, Chain::PolyMainnet, true)
            .unwrap();
        let conn_ref = conn.as_ref();
        let err = read_row_receivable(conn_ref, wallet_address).unwrap_err();
        match err {
            rusqlite::Error::QueryReturnedNoRows => (),
            x => panic!("we expected 'query returned no rows' but got '{}'", x),
        }
        let amount_wei = i128::MAX;

        let result = insert_or_update_receivable(
            conn_ref,
            &InsertUpdateCoreReal,
            wallet_address,
            amount_wei,
        );

        assert_eq!(result, Ok(()));
        let (balance, last_timestamp) = read_row_receivable(conn_ref, wallet_address).unwrap();
        assert!(is_timely_around(last_timestamp));
        assert_eq!(balance, amount_wei)
    }

    #[test]
    fn insert_or_update_receivable_works_for_insert_negative() {
        let wallet_address = "xyz456";
        let path = ensure_node_home_directory_exists(
            "dao_shared_methods",
            "insert_or_update_receivable_works_for_insert_negative",
        );
        let conn = DbInitializerReal::default()
            .initialize(&path, Chain::PolyMainnet, true)
            .unwrap();
        let conn_ref = conn.as_ref();
        let err = read_row_receivable(conn_ref, wallet_address).unwrap_err();
        match err {
            rusqlite::Error::QueryReturnedNoRows => (),
            x => panic!("we expected 'query returned no rows' but got '{}'", x),
        }
        let amount_wei = -125_125;

        let result = insert_or_update_receivable(
            conn_ref,
            &InsertUpdateCoreReal,
            wallet_address,
            amount_wei,
        );

        assert_eq!(result, Ok(()));
        let (balance, last_timestamp) = read_row_receivable(conn_ref, wallet_address).unwrap();
        assert!(is_timely_around(last_timestamp));
        assert_eq!(balance, -125_125)
    }

    #[test]
    fn insert_or_update_receivable_works_for_update_positive() {
        let wallet_address = "xyz789";
        let path = ensure_node_home_directory_exists(
            "dao_shared_methods",
            "insert_or_update_receivable_works_for_update_positive",
        );
        let conn = DbInitializerReal::default()
            .initialize(&path, Chain::PolyMainnet, true)
            .unwrap();
        let conn_ref = conn.as_ref();
        insert_receivable_100_balance_100_last_time_stamp(conn_ref, wallet_address);
        let (balance, last_time_stamp) = read_row_receivable(conn_ref, wallet_address).unwrap();
        assert_eq!(balance, 100);
        assert_eq!(last_time_stamp, 100);
        let amount_wei = i128::MAX - 100;

        let result = insert_or_update_receivable(
            conn_ref,
            &InsertUpdateCoreReal,
            wallet_address,
            amount_wei,
        );

        assert_eq!(result, Ok(()));
        let (balance, last_timestamp) = read_row_receivable(conn_ref, wallet_address).unwrap();
        assert_eq!(last_timestamp, 100); //TODO this might be an old issue - do we really never want to update the timestamp? Can it ever disappear or the timestamp from the initial insertion persists forever?
        assert_eq!(balance, amount_wei + 100)
    }

    #[test]
    fn insert_or_update_receivable_works_for_update_negative() {
        let wallet_address = "xyz789";
        let path = ensure_node_home_directory_exists(
            "dao_shared_methods",
            "insert_or_update_receivable_works_for_update_negative",
        );
        let conn = DbInitializerReal::default()
            .initialize(&path, Chain::PolyMainnet, true)
            .unwrap();
        let conn_ref = conn.as_ref();
        insert_receivable_100_balance_100_last_time_stamp(conn_ref, wallet_address);
        let (balance, last_time_stamp) = read_row_receivable(conn_ref, wallet_address).unwrap();
        assert_eq!(balance, 100);
        assert_eq!(last_time_stamp, 100);
        let amount_wei = -123_100;

        let result = insert_or_update_receivable(
            conn_ref,
            &InsertUpdateCoreReal,
            wallet_address,
            amount_wei,
        );

        assert_eq!(result, Ok(()));
        let (balance, last_timestamp) = read_row_receivable(conn_ref, wallet_address).unwrap();
        assert_eq!(last_timestamp, 100); //TODO this might be an old issue - do we really never want to update the timestamp? Can it ever disappear or the timestamp from the initial insertion persists forever?
        assert_eq!(balance, -123_000)
    }

    #[test]
    fn insert_or_update_payable_works_for_insert_positive() {
        let wallet_address = "xyz2211";
        let path = ensure_node_home_directory_exists(
            "dao_shared_methods",
            "insert_or_update_payable_works_for_insert_positive",
        );
        let conn = DbInitializerReal::default()
            .initialize(&path, Chain::PolyMainnet, true)
            .unwrap();
        let conn_ref = conn.as_ref();
        let err = read_row_payable(conn_ref, wallet_address).unwrap_err();
        match err {
            rusqlite::Error::QueryReturnedNoRows => (),
            x => panic!("we expected 'query returned no rows' but got '{}'", x),
        }
        let amount_wei = i128::MAX;

        let result =
            insert_or_update_payable(conn_ref, &InsertUpdateCoreReal, wallet_address, amount_wei);

        assert_eq!(result, Ok(()));
        let (balance, last_timestamp, pending_transaction_hash) =
            read_row_payable(conn_ref, wallet_address).unwrap();
        assert!(is_timely_around(last_timestamp));
        assert_eq!(balance, amount_wei);
        assert_eq!(pending_transaction_hash, None)
    }

    #[test]
    fn insert_or_update_payable_works_for_insert_negative() {
        let wallet_address = "xyz2211";
        let path = ensure_node_home_directory_exists(
            "dao_shared_methods",
            "insert_or_update_payable_works_for_insert_negative",
        );
        let conn = DbInitializerReal::default()
            .initialize(&path, Chain::PolyMainnet, true)
            .unwrap();
        let conn_ref = conn.as_ref();
        let err = read_row_payable(conn_ref, wallet_address).unwrap_err();
        match err {
            rusqlite::Error::QueryReturnedNoRows => (),
            x => panic!("we expected 'query returned no rows' but got '{}'", x),
        }
        let amount_wei = -1_245_999;

        let result =
            insert_or_update_payable(conn_ref, &InsertUpdateCoreReal, wallet_address, amount_wei);

        assert_eq!(result, Ok(()));
        let (balance, last_timestamp, pending_transaction_hash) =
            read_row_payable(conn_ref, wallet_address).unwrap();
        assert!(is_timely_around(last_timestamp));
        assert_eq!(balance, -1_245_999);
        assert_eq!(pending_transaction_hash, None)
    }

    #[test]
    fn insert_or_update_payable_works_for_update_positive() {
        let wallet_address = "xyz78978";
        let path = ensure_node_home_directory_exists(
            "dao_shared_methods",
            "insert_or_update_payable_works_for_update_positive",
        );
        let conn = DbInitializerReal::default()
            .initialize(&path, Chain::PolyMainnet, true)
            .unwrap();
        let conn_ref = conn.as_ref();
        insert_payable_100_balance_100_last_time_stamp_transaction_hash_abc(
            conn_ref,
            wallet_address,
        );
        let (balance, last_time_stamp, pending_transaction_hash) =
            read_row_payable(conn_ref, wallet_address).unwrap();
        assert_eq!(balance, 100);
        assert_eq!(last_time_stamp, 100);
        assert_eq!(pending_transaction_hash, Some("abc".to_string()));
        let amount_wei = i128::MAX - 100;

        let result =
            insert_or_update_payable(conn_ref, &InsertUpdateCoreReal, wallet_address, amount_wei);

        assert_eq!(result, Ok(()));
        let (balance, last_timestamp, pending_transaction_hash) =
            read_row_payable(conn_ref, wallet_address).unwrap();
        assert_eq!(last_timestamp, 100); //TODO this might be an old issue - do we really never want to update the timestamp? Can it ever disappear or the timestamp from the initial insertion persists forever?
        assert_eq!(balance, amount_wei + 100);
        assert_eq!(pending_transaction_hash, Some("abc".to_string()))
    }

    #[test]
    fn insert_or_update_payable_works_for_update_negative() {
        let wallet_address = "xyz78978";
        let path = ensure_node_home_directory_exists(
            "dao_shared_methods",
            "insert_or_update_payable_works_for_update_negative",
        );
        let conn = DbInitializerReal::default()
            .initialize(&path, Chain::PolyMainnet, true)
            .unwrap();
        let conn_ref = conn.as_ref();
        insert_payable_100_balance_100_last_time_stamp_transaction_hash_abc(
            conn_ref,
            wallet_address,
        );
        let (balance, last_time_stamp, pending_transaction_hash) =
            read_row_payable(conn_ref, wallet_address).unwrap();
        assert_eq!(balance, 100);
        assert_eq!(last_time_stamp, 100);
        assert_eq!(pending_transaction_hash, Some("abc".to_string()));
        let amount_wei = -90_333;

        let result =
            insert_or_update_payable(conn_ref, &InsertUpdateCoreReal, wallet_address, amount_wei);

        assert_eq!(result, Ok(()));
        let (balance, last_timestamp, pending_transaction_hash) =
            read_row_payable(conn_ref, wallet_address).unwrap();
        assert_eq!(last_timestamp, 100); //TODO this might be an old issue - do we really never want to update the timestamp? Can it ever disappear or the timestamp from the initial insertion persists forever?
        assert_eq!(balance, -90_233);
        assert_eq!(pending_transaction_hash, Some("abc".to_string()))
    }

    #[test]
    fn insert_or_update_payable_after_our_payment_works_for_insert_positive() {
        let wallet_address = "xyz2211";
        let path = ensure_node_home_directory_exists(
            "dao_shared_methods",
            "insert_or_update_payable_after_our_payment_works_for_insert_positive",
        );
        let conn = DbInitializerReal::default()
            .initialize(&path, Chain::PolyMainnet, true)
            .unwrap();
        let conn_ref = conn.as_ref();
        let err = read_row_payable(conn_ref, wallet_address).unwrap_err();
        match err {
            rusqlite::Error::QueryReturnedNoRows => (),
            x => panic!("we expected 'query returned no rows' but got '{}'", x),
        }
        let amount_wei = i128::MAX;
        let last_paid_timestamp = 10_000_000;
        let transaction_hash = "ce5456ed";

        let result = insert_or_update_payable_after_our_payment(
            conn_ref,
            &InsertUpdateCoreReal,
            wallet_address,
            amount_wei,
            last_paid_timestamp,
            transaction_hash,
        );

        assert_eq!(result, Ok(()));
        let (balance, last_timestamp, pending_transaction_hash) =
            read_row_payable(conn_ref, wallet_address).unwrap();
        assert_eq!(last_timestamp, last_paid_timestamp); //TODO this is going to produce troubles
        assert_eq!(balance, amount_wei);
        assert_eq!(pending_transaction_hash, Some(transaction_hash.to_string()))
    }

    #[test]
    fn insert_or_update_payable_after_our_payment_works_for_insert_negative() {
        let wallet_address = "xyz2211";
        let path = ensure_node_home_directory_exists(
            "dao_shared_methods",
            "insert_or_update_payable_after_our_payment_works_for_insert_negative",
        );
        let conn = DbInitializerReal::default()
            .initialize(&path, Chain::PolyMainnet, true)
            .unwrap();
        let conn_ref = conn.as_ref();
        let err = read_row_payable(conn_ref, wallet_address).unwrap_err();
        match err {
            rusqlite::Error::QueryReturnedNoRows => (),
            x => panic!("we expected 'query returned no rows' but got '{}'", x),
        }
        let amount_wei = -1_245_100;
        let last_paid_timestamp = 890_000_000;
        let transaction_hash = "ce5456ed";

        let result = insert_or_update_payable_after_our_payment(
            conn_ref,
            &InsertUpdateCoreReal,
            wallet_address,
            amount_wei,
            last_paid_timestamp,
            transaction_hash,
        );

        assert_eq!(result, Ok(()));
        let (balance, last_timestamp, pending_transaction_hash) =
            read_row_payable(conn_ref, wallet_address).unwrap();
        assert_eq!(last_timestamp, last_paid_timestamp);
        assert_eq!(balance, -1_245_100);
        assert_eq!(pending_transaction_hash, Some(transaction_hash.to_string()))
    }

    #[test]
    fn insert_or_update_payable_after_our_payment_works_for_update_positive() {
        let wallet_address = "xyz7894";
        let path = ensure_node_home_directory_exists(
            "dao_shared_methods",
            "insert_or_update_payable_after_our_payment_works_for_update_positive",
        );
        let conn = DbInitializerReal::default()
            .initialize(&path, Chain::PolyMainnet, true)
            .unwrap();
        let conn_ref = conn.as_ref();
        insert_payable_100_balance_100_last_time_stamp_transaction_hash_abc(
            conn_ref,
            wallet_address,
        );
        let (balance, last_time_stamp, pending_transaction_hash) =
            read_row_payable(conn_ref, wallet_address).unwrap();
        assert_eq!(balance, 100);
        assert_eq!(last_time_stamp, 100);
        assert_eq!(pending_transaction_hash, Some("abc".to_string()));
        let amount_wei = i128::MAX - 100;
        let last_paid_timestamp = 5_000;
        let transaction_hash = "ed78acc54";

        let result = insert_or_update_payable_after_our_payment(
            conn_ref,
            &InsertUpdateCoreReal,
            wallet_address,
            amount_wei,
            last_paid_timestamp,
            transaction_hash,
        );

        assert_eq!(result, Ok(()));
        let (balance, last_timestamp, pending_transaction_hash) =
            read_row_payable(conn_ref, wallet_address).unwrap();
        assert_eq!(last_timestamp, last_paid_timestamp);
        assert_eq!(balance, amount_wei + 100);
        assert_eq!(pending_transaction_hash, Some(transaction_hash.to_string()))
    }

    #[test]
    fn insert_or_update_payable_after_our_payment_works_for_update_negative() {
        let wallet_address = "xyz7894";
        let path = ensure_node_home_directory_exists(
            "dao_shared_methods",
            "insert_or_update_payable_after_our_payment_works_for_update_negative",
        );
        let conn = DbInitializerReal::default()
            .initialize(&path, Chain::PolyMainnet, true)
            .unwrap();
        let conn_ref = conn.as_ref();
        insert_payable_100_balance_100_last_time_stamp_transaction_hash_abc(
            conn_ref,
            wallet_address,
        );
        let (balance, last_time_stamp, pending_transaction_hash) =
            read_row_payable(conn_ref, wallet_address).unwrap();
        assert_eq!(balance, 100);
        assert_eq!(last_time_stamp, 100);
        assert_eq!(pending_transaction_hash, Some("abc".to_string()));
        let amount_wei = -45_000;
        let last_paid_timestamp = 5_000;
        let transaction_hash = "ed78acc54";

        let result = insert_or_update_payable_after_our_payment(
            conn_ref,
            &InsertUpdateCoreReal,
            wallet_address,
            amount_wei,
            last_paid_timestamp,
            transaction_hash,
        );

        assert_eq!(result, Ok(()));
        let (balance, last_timestamp, pending_transaction_hash) =
            read_row_payable(conn_ref, wallet_address).unwrap();
        assert_eq!(last_timestamp, last_paid_timestamp);
        assert_eq!(balance, -44_900);
        assert_eq!(pending_transaction_hash, Some(transaction_hash.to_string()))
    }

    #[test]
    fn update_receivable_error_handling_and_params_assertion() {
        let insert_or_update_params_arc = Arc::new(Mutex::new(vec![]));
        let wallet_address = "xyz123";
        let conn = Connection::open_in_memory().unwrap();
        let wrapped_conn = ConnectionWrapperReal::new(conn);
        create_broken_receivable(&wrapped_conn);
        let supplied_amount_wei = 100;
        let last_received_time_stamp_sec = 123;
        let insert_update_core = InsertUpdateCoreMock::default()
            .update_params(&insert_or_update_params_arc)
            .update_result(Err("SomethingWrong".to_string()));

        let result = update_receivable(
            &wrapped_conn,
            &insert_update_core,
            wallet_address,
            supplied_amount_wei,
            last_received_time_stamp_sec,
        );

        assert_eq!(
            result,
            Err(AccountantError::ReceivableError(
                ReceivableError::RusqliteError("SomethingWrong".to_string())
            ))
        );
        let mut insert_or_update_params = insert_or_update_params_arc.lock().unwrap();
        let (wallet, amount, (select_sql, update_sql, table, sql_param_names)) =
            insert_or_update_params.remove(0);
        assert!(insert_or_update_params.is_empty());
        assert_eq!(wallet, wallet_address);
        assert_eq!(amount, supplied_amount_wei);
        assert_eq!(
            select_sql,
            "select balance from receivable where wallet_address = :wallet"
        );
        assert_eq!(update_sql, "update receivable set balance = :updated_balance, last_received_timestamp = :last_received where wallet_address = :wallet");
        assert_eq!(table, Table::Receivable.to_string());
        assert_eq!(
            sql_param_names,
            [":wallet", ":last_received"].array_of_borrows_to_vec()
        )
    }

    #[test]
    fn insert_or_update_receivable_error_handling_and_params_assertion() {
        let insert_or_update_params_arc = Arc::new(Mutex::new(vec![]));
        let wallet_address = "xyz123";
        let conn = Connection::open_in_memory().unwrap();
        let wrapped_conn = ConnectionWrapperReal::new(conn);
        create_broken_receivable(&wrapped_conn);
        let supplied_amount_wei = 100;
        let insert_update_core = InsertUpdateCoreMock::default()
            .insert_or_update_params(&insert_or_update_params_arc)
            .insert_or_update_results(Err("SomethingWrong".to_string()));

        let result = insert_or_update_receivable(
            &wrapped_conn,
            &insert_update_core,
            wallet_address,
            supplied_amount_wei,
        );

        assert_eq!(
            result,
            Err(AccountantError::ReceivableError(
                ReceivableError::RusqliteError("SomethingWrong".to_string())
            ))
        );
        let mut insert_or_update_params = insert_or_update_params_arc.lock().unwrap();
        let (wallet, amount, (update_sql, insert_sql, table, sql_param_names)) =
            insert_or_update_params.remove(0);
        assert!(insert_or_update_params.is_empty());
        assert_eq!(wallet, wallet_address);
        assert_eq!(amount, supplied_amount_wei);
        assert_eq!(
            update_sql,
            "update receivable set balance = :updated_balance where wallet_address = :wallet"
        );
        assert_eq!(insert_sql,"insert into receivable (wallet_address, balance, last_received_timestamp) values (:wallet,:balance,strftime('%s','now'))");
        assert_eq!(table, Table::Receivable);
        assert_eq!(
            sql_param_names,
            [":wallet", ":balance"].array_of_borrows_to_vec()
        )
    }

    #[test]
    fn insert_or_update_payable_error_handling_and_params_assertion() {
        let insert_or_update_params_arc = Arc::new(Mutex::new(vec![]));
        let wallet_address = "xyz123";
        let conn = Connection::open_in_memory().unwrap();
        let wrapped_conn = ConnectionWrapperReal::new(conn);
        create_broken_payable(&wrapped_conn);
        let supplied_amount_wei = 100;
        let insert_update_core = InsertUpdateCoreMock::default()
            .insert_or_update_params(&insert_or_update_params_arc)
            .insert_or_update_results(Err("SomethingWrong".to_string()));

        let result = insert_or_update_payable(
            &wrapped_conn,
            &insert_update_core,
            wallet_address,
            supplied_amount_wei,
        );

        assert_eq!(
            result,
            Err(AccountantError::PayableError(PayableError::RusqliteError(
                "SomethingWrong".to_string()
            )))
        );
        let mut insert_or_update_params = insert_or_update_params_arc.lock().unwrap();
        let (wallet, amount, (update_sql, insert_sql, table, sql_param_names)) =
            insert_or_update_params.remove(0);
        assert!(insert_or_update_params.is_empty());
        assert_eq!(wallet, wallet_address);
        assert_eq!(amount, supplied_amount_wei);
        assert_eq!(
            update_sql,
            "update payable set balance = :updated_balance where wallet_address = :wallet"
        );
        assert_eq!(insert_sql,"insert into payable (wallet_address, balance, last_paid_timestamp, pending_payment_transaction) values (:wallet,:balance,strftime('%s','now'),null)");
        assert_eq!(table, Table::Payable);
        assert_eq!(
            sql_param_names,
            [":wallet", ":balance"].array_of_borrows_to_vec()
        )
    }

    #[test]
    fn insert_or_update_payable_after_our_payment_error_handling_and_params_assertion() {
        let insert_or_update_params_arc = Arc::new(Mutex::new(vec![]));
        let wallet_address = "xyz123";
        let conn = Connection::open_in_memory().unwrap();
        let wrapped_conn = ConnectionWrapperReal::new(conn);
        create_broken_payable(&wrapped_conn);
        let supplied_amount_wei = 100;
        let last_received_time_stamp_sec = 123;
        let tx_hash = "ab45cab45";
        let insert_update_core = InsertUpdateCoreMock::default()
            .insert_or_update_params(&insert_or_update_params_arc)
            .insert_or_update_results(Err("SomethingWrong".to_string()));

        let result = insert_or_update_payable_after_our_payment(
            &wrapped_conn,
            &insert_update_core,
            wallet_address,
            supplied_amount_wei,
            last_received_time_stamp_sec,
            tx_hash,
        );

        assert_eq!(
            result,
            Err(AccountantError::PayableError(PayableError::RusqliteError(
                "SomethingWrong".to_string()
            )))
        );
        let mut insert_or_update_params = insert_or_update_params_arc.lock().unwrap();
        let (wallet, amount, (update_sql, insert_sql, table, sql_param_names)) =
            insert_or_update_params.remove(0);
        assert!(insert_or_update_params.is_empty());
        assert_eq!(wallet, wallet_address);
        assert_eq!(amount, supplied_amount_wei);
        assert_eq!(update_sql,"update payable set balance = :updated_balance, last_paid_timestamp = :last_paid, pending_payment_transaction = :transaction where wallet_address = :wallet");
        assert_eq!(insert_sql,"insert into payable (balance, last_paid_timestamp, pending_payment_transaction, wallet_address) values (:balance, :last_paid, :transaction, :wallet)");
        assert_eq!(table, Table::Payable);
        assert_eq!(
            sql_param_names,
            [":wallet", ":balance", ":last_paid", ":transaction"].array_of_borrows_to_vec()
        )
    }

    #[test]
    fn update_handles_error_for_insert_update_config() {
        let wallet_address = "a11122";
        let conn = Connection::open_in_memory().unwrap();
        let wrapped_conn = ConnectionWrapperReal::new(conn);
        create_broken_payable(&wrapped_conn);
        let amount_wei = 100;
        let update_config = InsertUpdateConfig {
            insert_sql: "",
            update_sql: "",
            params: &[],
            table: Table::Payable,
        };

        let result =
            InsertUpdateCoreReal.update(&wrapped_conn, wallet_address, amount_wei, &update_config);

        assert_eq!(result, Err("Updating balance for payable of 100 Wei to a11122; failing on: 'Query returned no rows'".to_string()));
    }

    #[test]
    fn update_handles_error_on_a_row_due_to_unfitting_data_types() {
        let wallet_address = "a11122";
        let path = ensure_node_home_directory_exists(
            "dao_shared_methods",
            "update_handles_error_on_a_row_due_to_unfitting_data_types",
        );
        let conn = DbInitializerReal::default()
            .initialize(&path, Chain::PolyMainnet, true)
            .unwrap();
        let conn_ref = conn.as_ref();
        insert_payable_balance_last_time_stamp_transaction_hash_with_bad_data_types(
            conn_ref,
            wallet_address,
        );
        let amount_wei = 100;
        let last_received_time_stamp_sec = 123;
        let update_config = UpdateConfig {
            update_sql: "update receivable set balance = :updated_balance, last_received_timestamp = :last_received where wallet_address = :wallet",
            params: &[(":wallet",&wallet_address),(":last_received",&last_received_time_stamp_sec)],
            table: Table::Payable
        };

        let result =
            InsertUpdateCoreReal.update(conn_ref, wallet_address, amount_wei, &update_config);

        assert_eq!(result, Err("Updating balance for payable of 100 Wei to a11122; failing on: 'Invalid column type Text at index: 0, name: balance'".to_string()));
    }

    #[test]
    fn update_handles_error_on_a_row_due_to_bad_sql_params() {
        let wallet_address = "a11122";
        let path = ensure_node_home_directory_exists(
            "dao_shared_methods",
            "update_handles_error_on_a_row_due_to_bad_sql_params",
        );
        let conn = DbInitializerReal::default()
            .initialize(&path, Chain::PolyMainnet, true)
            .unwrap();
        let conn_ref = conn.as_ref();
        let mut stm = conn_ref.prepare("insert into payable (wallet_address, balance, last_paid_timestamp, pending_payment_transaction) values (?,?,strftime('%s','now'),null)").unwrap();
        stm.execute(params![wallet_address, 45245_i128]).unwrap();
        let amount_wei = 100;
        let last_received_time_stamp_sec = 123;
        let update_config = UpdateConfig {
            update_sql: "update receivable set balance = ?, last_received_timestamp = ? where wallet_address = ?",
            params: &[(":woodstock",&wallet_address),(":hendrix",&last_received_time_stamp_sec)],
            table: Table::Payable
        };

        let result =
            InsertUpdateCoreReal.update(conn_ref, wallet_address, amount_wei, &update_config);

        assert_eq!(result, Err("Updating balance for payable of 100 Wei to a11122; failing on: 'Invalid parameter name: :updated_balance'".to_string()));
    }

    #[test]
    fn reverse_sign_works_for_min_value(){
        let result = reverse_sign(i128::MIN);

        assert_eq!(result,Err(SignConversionError::I128(String::from("Reversing the sign for value: -170141183460469231731687303715884105728"))))
    }

    fn create_broken_receivable(conn: &dyn ConnectionWrapper) {
        let mut stm = conn.prepare(
            "create table receivable (
                wallet_address integer primary key,
                balance text null,
                last_received_timestamp integer not null
            )"
        )
        .unwrap();
        stm.execute([]).unwrap();
    }

    fn create_broken_payable(conn: &dyn ConnectionWrapper) {
        let mut stm = conn.prepare(
            "create table payable (
                wallet_address integer primary key,
                balance text not null,
                last_paid_timestamp integer not null,
                pending_payment_transaction integer null
            )"
        )
            .unwrap();
        stm.execute([]).unwrap();
    }

    fn insert_receivable_100_balance_100_last_time_stamp(
        conn: &dyn ConnectionWrapper,
        wallet: &str,
    ) {
        let params = named_params! {
            ":wallet":wallet,
            ":balance":100_i128,
            ":last_time_stamp":100_i64
        };
        let mut stm = conn.prepare("insert into receivable (wallet_address, balance, last_received_timestamp) values (:wallet,:balance,:last_time_stamp)").unwrap();
        stm.execute(params).unwrap();
    }

    fn insert_payable_100_balance_100_last_time_stamp_transaction_hash_abc(
        conn: &dyn ConnectionWrapper,
        wallet: &str,
    ) {
        let params = named_params! {
            ":wallet":wallet,
            ":balance":100_i128,
            ":last_time_stamp":100_i64,
            ":pending_payment_hash":"abc"
        };
        let mut stm = conn.prepare("insert into payable (wallet_address, balance, last_paid_timestamp, pending_payment_transaction) values (:wallet,:balance,:last_time_stamp,:pending_payment_hash)").unwrap();
        stm.execute(params).unwrap();
    }

    fn insert_payable_balance_last_time_stamp_transaction_hash_with_bad_data_types(
        conn: &dyn ConnectionWrapper,
        wallet: &str,
    ) {
        let params = named_params! {
            ":wallet":wallet,
            ":balance":"bubblebooo",
            ":last_time_stamp":"genesis",
            ":pending_payment_hash":45
        };
        let mut stm = conn.prepare("insert into payable (wallet_address, balance, last_paid_timestamp, pending_payment_transaction) values (:wallet,:balance,:last_time_stamp,:pending_payment_hash)").unwrap();
        stm.execute(params).unwrap();
    }

    fn read_row_receivable(
        conn: &dyn ConnectionWrapper,
        wallet_address: &str,
    ) -> rusqlite::Result<(i128, i64)> {
        let mut stm = conn
            .prepare(
                "select balance, last_received_timestamp from receivable where wallet_address = ?",
            )
            .unwrap();
        stm.query_row([wallet_address], |row| {
            let balance: rusqlite::Result<i128> = row.get(0);
            let last_received_timestamp = row.get(1);
            Ok((balance.unwrap(), last_received_timestamp.unwrap()))
        })
    }

    fn read_row_payable(
        conn: &dyn ConnectionWrapper,
        wallet_address: &str,
    ) -> rusqlite::Result<(i128, i64, Option<String>)> {
        let mut stm = conn.prepare("select balance, last_paid_timestamp, pending_payment_transaction from payable where wallet_address = ?").unwrap();
        stm.query_row([wallet_address], |row| {
            let balance: rusqlite::Result<i128> = row.get(0);
            let last_received_timestamp = row.get(1);
            let pending_transaction_hash: rusqlite::Result<Option<String>> = row.get(2);
            Ok((
                balance.unwrap(),
                last_received_timestamp.unwrap(),
                pending_transaction_hash.unwrap(),
            ))
        })
    }

    fn is_timely_around(time_stamp: i64) -> bool {
        let new_now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        (new_now - 3) < time_stamp && time_stamp <= new_now
    }
}
