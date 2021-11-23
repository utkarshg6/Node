// Copyright (c) 2019, MASQ (https://masq.ai) and/or its affiliates. All rights reserved.

use crate::accountant::AccountantError;
use crate::database::connection_wrapper::ConnectionWrapper;
use masq_lib::utils::WrapResult;
use rusqlite::types::Value;
use rusqlite::{Connection, ToSql};
use std::fmt::{Display, Formatter};

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

fn insert_or_update(
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
        Ok(rows) => return Ok(()),
        Err(e) => {
            let cause = e.to_string();
            let constraint_failed = "UNIQUE constraint failed";
            if &cause[..constraint_failed.len()] == constraint_failed {
                update(conn, wallet, amount, &config)
            } else {
                unimplemented!("{}", cause)
            }
        }
    }
}

pub fn blob_i128(amount: i128) -> Value {
    amount.into()
}

pub fn update(
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
                let blob = blob_i128(updated_balance);
                let mut adjusted_params = config.update_params().to_vec();
                config.should_balance_param_be_removed(&mut adjusted_params);
                adjusted_params.insert(0, (":updated_balance", &blob));
                let mut stm = conn
                    .prepare(config.update_sql())
                    .expect("internal rusqlite error");
                stm.execute(&*adjusted_params)
                    .unwrap_or_else(|e| unimplemented!())
                    .wrap_to_ok()
            }
            Err(e) => {
                unimplemented!()
            }
        }
    }) {
        Ok(_) => Ok(()),
        Err(e) => unimplemented!("{}", e),
    }
}

pub trait UpdateConfiguration<'a> {
    fn select_sql(&self) -> String;
    fn update_sql(&self) -> &'a str;
    fn update_params(&self) -> &'a [(&'static str, &'a dyn ToSql)];
    fn should_balance_param_be_removed(&self, params_to_adjust: &mut Vec<(&str, &dyn ToSql)>);
}

impl<'a> UpdateConfiguration<'a> for InsertUpdateConfig<'a> {
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
fn update_receivable(
    conn: &dyn ConnectionWrapper,
    wallet: &str,
    amount: i128,
    last_received_time_stamp: i64,
) -> Result<(), AccountantError> {
    let config = UpdateConfig {
        update_sql: "update receivable set balance = :updated_balance, last_received_timestamp = :last_received where wallet_address = :wallet",
        params: &[(":wallet",&wallet),(":last_received",&last_received_time_stamp)],
        table: Table::Receivable
    };
    update(conn, wallet, amount, &config).map_err(|e| unimplemented!())
}

//insert_update
fn insert_or_update_receivable(
    conn: &dyn ConnectionWrapper,
    wallet: &str,
    amount: i128,
) -> Result<(), AccountantError> {
    let config = InsertUpdateConfig{
        insert_sql: "insert into receivable (wallet_address, balance, last_received_timestamp) values (:wallet,:balance,strftime('%s','now'))",
        update_sql: "update receivable set balance = :updated_balance where wallet_address = :wallet",
        params: &[(":wallet", &wallet), (":balance", &amount)],
        table: Table::Receivable
    };
    insert_or_update(conn, wallet, amount, config).map_err(|e| unimplemented!())
}

pub fn insert_or_update_payable(
    conn: &dyn ConnectionWrapper,
    wallet: &str,
    amount: i128,
) -> Result<(), AccountantError> {
    let config = InsertUpdateConfig{
        insert_sql: "insert into payable (wallet_address, balance, last_paid_timestamp, pending_payment_transaction) values (:wallet,:balance,strftime('%s','now'),null)",
        update_sql: "update payable set balance = :updated_balance where wallet_address = :wallet",
        params: &[(":wallet", &wallet), (":balance", &amount)],
        table: Table::Payable
    };
    insert_or_update(conn, wallet, amount, config).map_err(|e| unimplemented!())
}

pub fn insert_or_update_payable_from_our_payment(
    conn: &dyn ConnectionWrapper,
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
    insert_or_update(conn, wallet, amount, config).map_err(|e| unimplemented!())
}

#[cfg(test)]
mod tests {
    use crate::accountant::dao_shared_methods::{
        insert_or_update_payable, insert_or_update_payable_from_our_payment,
        insert_or_update_receivable, update_receivable, Table,
    };
    use crate::database::connection_wrapper::ConnectionWrapper;
    use crate::database::db_initializer::{DbInitializer, DbInitializerReal};
    use masq_lib::blockchains::chains::Chain;
    use masq_lib::constants::DEFAULT_CHAIN;
    use masq_lib::test_utils::utils::ensure_node_home_directory_exists;
    use rusqlite::{named_params, Connection};
    use std::time::SystemTime;

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

        let result = insert_or_update_receivable(conn_ref, wallet_address, amount_wei);

        assert_eq!(result, Ok(()));
        let (balance, last_timestamp) = read_row_receivable(conn_ref, wallet_address).unwrap();
        assert_eq!(
            last_timestamp,
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
        ); //TODO this is going to produce troubles
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

        let result = insert_or_update_receivable(conn_ref, wallet_address, amount_wei);

        assert_eq!(result, Ok(()));
        let (balance, last_timestamp) = read_row_receivable(conn_ref, wallet_address).unwrap();
        assert_eq!(
            last_timestamp,
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
        ); //TODO this is going to produce troubles
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

        let result = insert_or_update_receivable(conn_ref, wallet_address, amount_wei);

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

        let result = insert_or_update_receivable(conn_ref, wallet_address, amount_wei);

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

        let result = insert_or_update_payable(conn_ref, wallet_address, amount_wei);

        assert_eq!(result, Ok(()));
        let (balance, last_timestamp, pending_transaction_hash) =
            read_row_payable(conn_ref, wallet_address).unwrap();
        assert_eq!(
            last_timestamp,
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
        ); //TODO this is going to produce troubles
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

        let result = insert_or_update_payable(conn_ref, wallet_address, amount_wei);

        assert_eq!(result, Ok(()));
        let (balance, last_timestamp, pending_transaction_hash) =
            read_row_payable(conn_ref, wallet_address).unwrap();
        assert_eq!(
            last_timestamp,
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
        ); //TODO this is going to produce troubles
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

        let result = insert_or_update_payable(conn_ref, wallet_address, amount_wei);

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

        let result = insert_or_update_payable(conn_ref, wallet_address, amount_wei);

        assert_eq!(result, Ok(()));
        let (balance, last_timestamp, pending_transaction_hash) =
            read_row_payable(conn_ref, wallet_address).unwrap();
        assert_eq!(last_timestamp, 100); //TODO this might be an old issue - do we really never want to update the timestamp? Can it ever disappear or the timestamp from the initial insertion persists forever?
        assert_eq!(balance, -90_233);
        assert_eq!(pending_transaction_hash, Some("abc".to_string()))
    }

    #[test]
    fn insert_or_update_payable_from_payment_works_for_insert_positive() {
        let wallet_address = "xyz2211";
        let path = ensure_node_home_directory_exists(
            "dao_shared_methods",
            "insert_or_update_payable_from_payment_works_for_insert_positive",
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

        let result = insert_or_update_payable_from_our_payment(
            conn_ref,
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
    fn insert_or_update_payable_from_payment_works_for_insert_negative() {
        let wallet_address = "xyz2211";
        let path = ensure_node_home_directory_exists(
            "dao_shared_methods",
            "insert_or_update_payable_from_payment_works_for_insert_negative",
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

        let result = insert_or_update_payable_from_our_payment(
            conn_ref,
            wallet_address,
            amount_wei,
            last_paid_timestamp,
            transaction_hash,
        );

        assert_eq!(result, Ok(()));
        let (balance, last_timestamp, pending_transaction_hash) =
            read_row_payable(conn_ref, wallet_address).unwrap();
        assert_eq!(last_timestamp, last_paid_timestamp); //TODO this is going to produce troubles
        assert_eq!(balance, -1_245_100);
        assert_eq!(pending_transaction_hash, Some(transaction_hash.to_string()))
    }

    #[test]
    fn insert_or_update_payable_from_our_payment_works_for_update_positive() {
        let wallet_address = "xyz7894";
        let path = ensure_node_home_directory_exists(
            "dao_shared_methods",
            "insert_or_update_payable_from_our_payment_works_for_update_positive",
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

        let result = insert_or_update_payable_from_our_payment(
            conn_ref,
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
    fn insert_or_update_payable_from_our_payment_works_for_update_negative() {
        let wallet_address = "xyz7894";
        let path = ensure_node_home_directory_exists(
            "dao_shared_methods",
            "insert_or_update_payable_from_our_payment_works_for_update_negative",
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

        let result = insert_or_update_payable_from_our_payment(
            conn_ref,
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

    fn read_balance(conn: &dyn ConnectionWrapper, wallet: &str, table: Table) -> i128 {
        let mut stm = conn
            .prepare(&format!(
                "select balance from {} where wallet_address = ?",
                table
            ))
            .unwrap();
        stm.query_row([wallet], |row| {
            let balance: rusqlite::Result<i128> = row.get(0);
            balance
        })
        .unwrap()
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
}
