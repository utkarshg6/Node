// Copyright (c) 2019, MASQ (https://masq.ai) and/or its affiliates. All rights reserved.
use crate::accountant::dao_shared_methods::{
    insert_or_update_receivable, reverse_sign, update_receivable, InsertUpdateCore,
};
use crate::accountant::u128_to_signed;
use crate::accountant::{u64_to_signed, AccountantError, PaymentCurves, SignConversionError};
use crate::blockchain::blockchain_interface::Transaction;
use crate::database::connection_wrapper::ConnectionWrapper;
use crate::database::dao_utils;
use crate::database::dao_utils::{to_time_t, DaoFactoryReal, now_time_t};
use crate::db_config::config_dao::{ConfigDaoWrite, ConfigDaoWriteableReal};
use crate::db_config::persistent_configuration::PersistentConfigError;
use crate::sub_lib::logger::Logger;
use crate::sub_lib::wallet::Wallet;
use indoc::indoc;
use masq_lib::utils::WrapResult;
use rusqlite::named_params;
use rusqlite::types::{ToSql, Type};
use rusqlite::{OptionalExtension, Row};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::accountant::receivable_dao::ReceivableError::RusqliteError;

#[derive(Debug, PartialEq)]
pub enum ReceivableError {
    Overflow(SignConversionError),
    ConfigurationError(String),
    RusqliteError(String), // TODO you will have to pick up from different cases and give it to here or Other
    Other(String),
}

impl From<PersistentConfigError> for ReceivableError {
    fn from(input: PersistentConfigError) -> Self {
        ReceivableError::ConfigurationError(format!("{:?}", input))
    }
}

impl From<String> for ReceivableError {
    fn from(input: String) -> Self {
        ReceivableError::Other(input)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReceivableAccount {
    pub wallet: Wallet,
    pub balance: i128,
    pub last_received_timestamp: SystemTime,
}

pub trait ReceivableDao: Send {
    fn more_money_receivable(
        &self,
        wallet: &Wallet,
        amount: u128,
        insert_update_core: &dyn InsertUpdateCore,
    ) -> Result<(), AccountantError>;

    fn more_money_received(
        &mut self,
        transactions: &[Transaction],
        insert_update_core: &dyn InsertUpdateCore,
    ) -> Result<(), AccountantError>;

    fn single_account_status(&self, wallet: &Wallet) -> Option<ReceivableAccount>;

    fn all_receivable_accounts(&self) -> Vec<ReceivableAccount>;

    fn new_delinquencies(
        &self,
        now: SystemTime,
        payment_curves: &PaymentCurves,
    ) -> Result<Vec<ReceivableAccount>, AccountantError>;

    fn paid_delinquencies(
        &self,
        payment_curves: &PaymentCurves,
    ) -> Result<Vec<ReceivableAccount>, AccountantError>;

    fn top_records(
        &self,
        minimum_amount: i128,
        maximum_age: u64,
    ) -> Result<Vec<ReceivableAccount>, AccountantError>;

    fn total(&self) -> i128;
}

pub trait ReceivableDaoFactory {
    fn make(&self) -> Box<dyn ReceivableDao>;
}

impl ReceivableDaoFactory for DaoFactoryReal {
    fn make(&self) -> Box<dyn ReceivableDao> {
        Box::new(ReceivableDaoReal::new(self.make_connection()))
    }
}

pub struct ReceivableDaoReal {
    conn: Box<dyn ConnectionWrapper>,
    logger: Logger,
}

impl ReceivableDao for ReceivableDaoReal {
    fn more_money_receivable(
        &self,
        wallet: &Wallet,
        amount: u128,
        insert_update_core: &dyn InsertUpdateCore,
    ) -> Result<(), AccountantError> {
        let signed_amount = u128_to_signed(amount).map_err(|e| {
            e.into_receivable().extend(&format!(
                "on calling more_money_receivable(); for wallet '{}' and amount '{}'",
                wallet, amount
            ))
        })?;
        match insert_or_update_receivable(
            self.conn.as_ref(),
            insert_update_core,
            &wallet.to_string(),
            signed_amount,
        ) {
            Ok(_) => Ok(()),
            Err(e) => unimplemented!(), //e.extend(&format!("on calling more_money_receivable(); for wallet '{}' and amount '{}'",wallet,amount)).wrap_to_err() //TODO you can make this compatible with the error from payables
        }
    }

    fn more_money_received(
        &mut self,
        payments: &[Transaction],
        insert_update_core: &dyn InsertUpdateCore,
    ) -> Result<(), AccountantError> {
        self.try_multi_insert_payment(payments, insert_update_core)
    }

    fn single_account_status(&self, wallet: &Wallet) -> Option<ReceivableAccount> {
        let mut stmt = self
            .conn
            .prepare(
                "select wallet_address, balance, last_received_timestamp from receivable where wallet_address = ?",
            )
            .expect("Internal error");
        match stmt.query_row(&[&wallet], Self::row_to_account).optional() {
            Ok(value) => value,
            Err(e) => panic!("Database is corrupt: {:?}", e),
        }
    }

    fn all_receivable_accounts(&self) -> Vec<ReceivableAccount> {
        let mut stmt = self
            .conn
            .prepare("select wallet_address, balance, last_received_timestamp from receivable")
            .expect("Internal error");

        stmt.query_map([], Self::row_to_account)
            .expect("Database is corrupt")
            .flatten()
            .collect()
    }

    fn new_delinquencies(
        &self,
        system_now: SystemTime,
        payment_curves: &PaymentCurves,
    ) -> Result<Vec<ReceivableAccount>, AccountantError> {
        self.create_temporary_table_with_metadata_for_yet_unbanned(payment_curves)?;
        let sql = indoc!(
            r"
            select r.wallet_address, r.balance, r.last_received_timestamp
            from receivable r left outer join delinquency_metadata d on r.wallet_address = d.wallet_address
            where
                r.last_received_timestamp < :sugg_and_grace
                and r.balance > d.curve_point
                and r.balance > :permanent_debt
        "
        );
        let mut stmt = self.conn.prepare(sql).expect("Couldn't prepare statement");
        stmt.query_map(
            named_params! {
                ":sugg_and_grace": payment_curves.sugg_and_grace(to_time_t(system_now)),
                ":permanent_debt": u128_to_signed(payment_curves.permanent_debt_allowed_wei).map_err(|e|e.into_receivable())?,
            },
            Self::row_to_account,
        )
        .expect("Couldn't retrieve new delinquencies: database corruption")
        .flatten()
        .collect::<Vec<ReceivableAccount>>().wrap_to_ok()
    }

    fn paid_delinquencies(
        &self,
        payment_curves: &PaymentCurves,
    ) -> Result<Vec<ReceivableAccount>, AccountantError> {
        let sql = indoc!(
            r"
            select r.wallet_address, r.balance, r.last_received_timestamp
            from receivable r inner join banned b on r.wallet_address = b.wallet_address
            where
                r.balance <= :unban_balance
        "
        );
        let mut stmt = self.conn.prepare(sql).expect("Couldn't prepare statement");
        stmt.query_map(
            named_params! {
                ":unban_balance": u128_to_signed(payment_curves.unban_when_balance_below_wei).map_err(|e|e.into_receivable())?,
            },
            Self::row_to_account,
        )
        .expect("Couldn't retrieve new delinquencies: database corruption")
        .flatten()
        .collect::<Vec<ReceivableAccount>>().wrap_to_ok()
    }

    fn top_records(
        &self,
        minimum_amount: i128,
        maximum_age: u64,
    ) -> Result<Vec<ReceivableAccount>, AccountantError> {
        let max_age = u64_to_signed(maximum_age).map_err(|e| e.into_receivable())?;
        let min_timestamp = dao_utils::now_time_t() - max_age;
        let mut stmt = self
            .conn
            .prepare(
                r#"
                select
                    balance,
                    last_received_timestamp,
                    wallet_address
                from
                    receivable
                where
                    balance >= ? and
                    last_received_timestamp >= ?
                order by
                    balance desc,
                    last_received_timestamp desc
            "#,
            )
            .expect("Internal error");
        let params: &[&dyn ToSql] = &[&minimum_amount, &min_timestamp];
        stmt.query_map(params, |row| {
            let balance_result = row.get(0);
            let last_paid_timestamp_result = row.get(1);
            let wallet_result: Result<Wallet, rusqlite::Error> = row.get(2);
            match (balance_result, last_paid_timestamp_result, wallet_result) {
                (Ok(balance), Ok(last_paid_timestamp), Ok(wallet)) => Ok(ReceivableAccount {
                    wallet,
                    balance,
                    last_received_timestamp: dao_utils::from_time_t(last_paid_timestamp),
                }),
                _ => panic!("Database is corrupt: RECEIVABLE table columns and/or types"),
            }
        })
        .expect("Database is corrupt")
        .flatten()
        .collect::<Vec<ReceivableAccount>>()
        .wrap_to_ok()
    }

    fn total(&self) -> i128 {
        let mut stmt = self
            .conn
            .prepare("select sum(balance) from receivable")
            .expect("Internal error");
        match stmt.query_row([], |row| {
            let total_balance_result: Result<i128, rusqlite::Error> = row.get(0);
            match total_balance_result {
                Ok(total_balance) => Ok(total_balance),
                Err(e)
                    if e == rusqlite::Error::InvalidColumnType(
                        0,
                        "sum(balance)".to_string(),
                        Type::Null,
                    ) =>
                {
                    Ok(0_i128)
                }
                Err(e) => panic!(
                    "Database is corrupt: RECEIVABLE table columns and/or types: {:?}",
                    e
                ),
            }
        }) {
            Ok(value) => value,
            Err(e) => panic!("Database is corrupt: {:?}", e),
        }
    }
}

impl ReceivableDaoReal {
    pub fn new(conn: Box<dyn ConnectionWrapper>) -> ReceivableDaoReal {
        ReceivableDaoReal {
            conn,
            logger: Logger::new("ReceivableDaoReal"),
        }
    }

    fn create_temporary_table_with_metadata_for_yet_unbanned(&self, payment_curves:&PaymentCurves) ->Result<(),AccountantError>{
        let sql = indoc!(
            r"
            create temp table delinquency_metadata(
                wallet_address text not null,
                curve_point blob not null
            )");
        let mut temp_table_stm = self.conn.prepare(sql).expect("internal error");
        temp_table_stm.execute([]).expect("creation of a temporary table failed");
        let slope = (payment_curves.permanent_debt_allowed_wei as f64
            - payment_curves.balance_to_decrease_from_wei as f64)
            / (payment_curves.balance_decreases_for_sec as f64);
        let mut select_stm = self.conn.prepare("select r.wallet_address, r.last_received_timestamp from receivable r left outer join banned b on r.wallet_address = b.wallet_address where b.wallet_address is null").expect("internal error");
        let mut insert_stm = self.conn.prepare("insert into delinquency_metadata (wallet_address, curve_point) values (:wallet_address, :curve_point)").expect("internal error");
        select_stm.query_map([],|row|{
            let wallet_address:rusqlite::Result<String> = row.get(0);
            let timestamp:rusqlite::Result<i64> = row.get(1);
            match (wallet_address,timestamp) {
                (Ok(wallet_address),Ok(timestamp)) => {
                    let declining_curve_boarder = Self::delinquency_curve_height_detection(payment_curves, now_time_t(), timestamp, slope).map_err(|e|{unimplemented!();rusqlite::Error::BlobSizeError})?;
                    eprintln!("declining boarder {}\n, {}",declining_curve_boarder, wallet_address);
                    let insert_params = named_params!(":wallet_address":&wallet_address,":curve_point":&declining_curve_boarder);
                    insert_stm.execute(insert_params).map_err(|e|{unimplemented!();rusqlite::Error::BlobSizeError})?;
                    Ok(())
                }
                _ => unimplemented!()
            }
        })
            .expect("internal error")
            .collect::<Vec<rusqlite::Result<()>>>();
        Ok(())
    }

    fn delinquency_curve_height_detection(payment_curves:&PaymentCurves,now: i64,timestamp:i64,slope: f64)->Result<i128,AccountantError>{
        Ok(u128_to_signed(payment_curves.balance_to_decrease_from_wei).map_err(|e|unimplemented!())?
        + slope as i128 * (payment_curves.sugg_and_grace( now) - timestamp) as i128)
    }

    fn try_multi_insert_payment(
        &mut self,
        payments: &[Transaction],
        insert_update_core: &dyn InsertUpdateCore,
    ) -> Result<(), AccountantError> {
        //this function is also tested from a higher level in accountant/mod.rs, at mentions of handle_received_payments
        let tx = match self.conn.transaction() {
            Ok(t) => t,
            Err(e) => {
                return Err(AccountantError::ReceivableError(ReceivableError::Other(
                    e.to_string(),
                )))
            }
        };

        let block_number = payments
            .iter()
            .map(|t| t.block_number)
            .max()
            .ok_or_else(|| {
                AccountantError::ReceivableError(ReceivableError::Other(
                    "no payments given".to_string(),
                ))
            })?;

        let mut writer = ConfigDaoWriteableReal::new(tx);
        match writer.set("start_block", Some(block_number.to_string())) {
            Ok(_) => (),
            Err(e) => return Err(AccountantError::ReceivableError(ReceivableError::ConfigurationError(format!("{:?}", e)))),
        }
        let tx = writer
            .extract()
            .expect("Transaction disappeared from writer");

        {
            for transaction in payments {
                let timestamp = dao_utils::now_time_t();
                let wei_amount = match u128_to_signed(transaction.wei_amount) {
                    Ok(amount) => amount,
                    Err(e) => return Err(e.into_receivable().extend("on calling try_multi_insert_payment()"))};
                let sign_reversed_amount = reverse_sign(wei_amount)
                    .expect("should be within the correct range after the previous operation");
                let wallet = transaction.from.to_string();
                update_receivable(
                    &tx,
                    insert_update_core,
                    &wallet,
                    sign_reversed_amount,
                    timestamp,
                )
                .map_err(|e| e
                    .extend("couldn't update an account calling try_multi_insert_payment()"))?;
            }
        }
        match tx.commit() {
            // Error response is untested here, because without a mockable Transaction, it's untestable.
            Err(e) => unimplemented!("test drive me back"), //Err(ReceivableError::Other(format!("{:?}", e))),
            Ok(_) => Ok(()),
        }
    }

    fn row_to_account(row: &Row) -> rusqlite::Result<ReceivableAccount> {
        let wallet: Result<Wallet, rusqlite::Error> = row.get(0);
        let balance_result = row.get(1);
        let last_received_timestamp_result = row.get(2);
        match (wallet, balance_result, last_received_timestamp_result) {
            (Ok(wallet), Ok(balance), Ok(last_received_timestamp)) => Ok(ReceivableAccount {
                wallet,
                balance,
                last_received_timestamp: dao_utils::from_time_t(last_received_timestamp),
            }),
            _ => panic!("Database is corrupt: RECEIVABLE table columns and/or types"),
        }
    }
}

//TODO implement this
pub trait TotalInnerEncapsulationReceivable {
    fn total_inner(&self, dao: &dyn ReceivableDao) -> Result<i128, AccountantError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accountant::dao_shared_methods::InsertUpdateCoreReal;
    use crate::accountant::test_utils::make_receivable_account;
    use crate::database::dao_utils::{from_time_t, now_time_t, to_time_t};
    use crate::database::db_initializer;
    use crate::database::db_initializer::test_utils::ConnectionWrapperMock;
    use crate::database::db_initializer::DbInitializer;
    use crate::database::db_initializer::DbInitializerReal;
    use crate::db_config::config_dao::ConfigDaoReal;
    use crate::db_config::persistent_configuration::{
        PersistentConfigError, PersistentConfiguration, PersistentConfigurationReal,
    };
    use crate::test_utils::assert_contains;
    use crate::test_utils::logging;
    use crate::test_utils::logging::TestLogHandler;
    use crate::test_utils::make_wallet;
    use masq_lib::test_utils::utils::{ensure_node_home_directory_exists, TEST_DEFAULT_CHAIN};
    use rusqlite::{Connection, Error, OpenFlags};
    use crate::database::connection_wrapper::ConnectionWrapperReal;

    #[test]
    fn conversion_from_pce_works() {
        let pce = PersistentConfigError::BadHexFormat("booga".to_string());

        let subject = ReceivableError::from(pce);

        assert_eq!(
            subject,
            ReceivableError::ConfigurationError("BadHexFormat(\"booga\")".to_string())
        );
    }

    #[test]
    fn conversion_from_string_works() {
        let subject = ReceivableError::from("booga".to_string());

        todo!("when all is green, try to discard this function if it was really needed");
        assert_eq!(subject, ReceivableError::Other("booga".to_string()));
    }

    #[test]
    fn try_multi_insert_payment_handles_error_from_amount_overflow() {
        let home_dir = ensure_node_home_directory_exists(
            "receivable_dao",
            "try_multi_insert_payment_handles_error_from_amount_overflow",
        );
        let mut subject = ReceivableDaoReal::new(
            DbInitializerReal::default()
                .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
                .unwrap(),
        );
        let payments = vec![Transaction {
            block_number: 42u64,
            from: make_wallet("some_address"),
            wei_amount: u128::MAX,
        }];

        let result = subject.try_multi_insert_payment(&payments.as_slice(), &InsertUpdateCoreReal);

        assert_eq!(
            result,
            Err(AccountantError::ReceivableError(ReceivableError::Overflow(SignConversionError::U128(
                "conversion of 340282366920938463463374607431768211455 from u128 to i128 failed on: out of range integral type conversion attempted; on calling try_multi_insert_payment()".to_string()
            ))))
        )
    }

    #[test]
    fn try_multi_insert_payment_handles_error_setting_start_block() {
        let home_dir = ensure_node_home_directory_exists(
            "receivable_dao",
            "try_multi_insert_payment_handles_error_setting_start_block",
        );
        let conn = Connection::open_in_memory().unwrap();
        let conn_wrapped = ConnectionWrapperReal::new(conn);
        let mut subject = ReceivableDaoReal::new(Box::new(conn_wrapped));
        let payments = vec![Transaction {
            block_number: 42u64,
            from: make_wallet("some_address"),
            wei_amount: 18446744073709551615,
        }];

        let result = subject.try_multi_insert_payment(&payments.as_slice(), &InsertUpdateCoreReal);

        assert_eq!(
            result,
            Err(AccountantError::ReceivableError(ReceivableError::ConfigurationError(
                "DatabaseError(\"no such table: config\")".to_string()
            )))
        )
    }

    #[test]
    #[should_panic(expected = "no such table: receivable")]
    fn try_multi_insert_payment_handles_error_adding_receivables() {
        let home_dir = ensure_node_home_directory_exists(
            "receivable_dao",
            "try_multi_insert_payment_handles_error_adding_receivables",
        );
        let conn = DbInitializerReal::default()
            .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
            .unwrap();
        {
            let mut stmt = conn.prepare("drop table receivable").unwrap();
            stmt.execute([]).unwrap();
        }
        let mut subject = ReceivableDaoReal::new(conn);

        let payments = vec![Transaction {
            block_number: 42u64,
            from: make_wallet("some_address"),
            wei_amount: 18446744073709551615,
        }];

        let _ = subject.try_multi_insert_payment(payments.as_slice(), &InsertUpdateCoreReal);
    }

    #[test]
    fn more_money_receivable_works_for_new_address() {
        let home_dir = ensure_node_home_directory_exists(
            "receivable_dao",
            "more_money_receivable_works_for_new_address",
        );
        let before = dao_utils::to_time_t(SystemTime::now());
        let wallet = make_wallet("booga");
        let status = {
            let subject = ReceivableDaoReal::new(
                DbInitializerReal::default()
                    .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
                    .unwrap(),
            );

            subject
                .more_money_receivable(&wallet, 1234, &InsertUpdateCoreReal)
                .unwrap();
            subject.single_account_status(&wallet).unwrap()
        };

        let after = dao_utils::to_time_t(SystemTime::now());
        assert_eq!(status.wallet, wallet);
        assert_eq!(status.balance, 1234);
        let timestamp = dao_utils::to_time_t(status.last_received_timestamp);
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
    fn more_money_receivable_works_for_existing_address() {
        let home_dir = ensure_node_home_directory_exists(
            "receivable_dao",
            "more_money_receivable_works_for_existing_address",
        );
        let wallet = make_wallet("booga");
        let subject = {
            let subject = ReceivableDaoReal::new(
                DbInitializerReal::default()
                    .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
                    .unwrap(),
            );
            subject
                .more_money_receivable(&wallet, 1234, &InsertUpdateCoreReal)
                .unwrap();
            let mut flags = OpenFlags::empty();
            flags.insert(OpenFlags::SQLITE_OPEN_READ_WRITE);
            let conn =
                Connection::open_with_flags(&home_dir.join(db_initializer::DATABASE_FILE), flags)
                    .unwrap();
            conn.execute(
                "update receivable set last_received_timestamp = 0 where wallet_address = '0x000000000000000000000000000000626f6f6761'",
                [],
            )
            .unwrap();
            subject
        };

        let status = {
            subject
                .more_money_receivable(&wallet, 2345, &InsertUpdateCoreReal)
                .unwrap();
            subject.single_account_status(&wallet).unwrap()
        };

        assert_eq!(status.wallet, wallet);
        assert_eq!(status.balance, 3579);
        assert_eq!(status.last_received_timestamp, SystemTime::UNIX_EPOCH);
    }

    #[test]
    fn more_money_receivable_works_for_overflow() {
        let home_dir = ensure_node_home_directory_exists(
            "receivable_dao",
            "more_money_receivable_works_for_overflow",
        );
        let subject = ReceivableDaoReal::new(
            DbInitializerReal::default()
                .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
                .unwrap(),
        );

        let result =
            subject.more_money_receivable(&make_wallet("booga"), u128::MAX, &InsertUpdateCoreReal);

        assert_eq!(
            result,
            Err(AccountantError::ReceivableError(ReceivableError::Overflow(
                SignConversionError::U128("conversion of 340282366920938463463374607431768211455 from u128 to i128 failed on: out of range integral type conversion attempted; on calling more_money_receivable(); for wallet '0x000000000000000000000000000000626f6f6761' and amount '340282366920938463463374607431768211455'".to_string())
            )))
        )
    }

    #[test]
    fn more_money_received_works_for_existing_addresses() {
        let before = dao_utils::to_time_t(SystemTime::now());
        let home_dir = ensure_node_home_directory_exists(
            "receivable_dao",
            "more_money_received_works_for_existing_address",
        );
        let debtor1 = make_wallet("debtor1");
        let debtor2 = make_wallet("debtor2");
        let mut subject = {
            let subject = ReceivableDaoReal::new(
                DbInitializerReal::default()
                    .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
                    .unwrap(),
            );
            subject
                .more_money_receivable(&debtor1, 1234, &InsertUpdateCoreReal)
                .unwrap();
            subject
                .more_money_receivable(&debtor2, 2345, &InsertUpdateCoreReal)
                .unwrap();
            let mut flags = OpenFlags::empty();
            flags.insert(OpenFlags::SQLITE_OPEN_READ_WRITE);
            subject
        };

        let (status1, status2) = {
            let transactions = vec![
                Transaction {
                    from: debtor1.clone(),
                    wei_amount: 1200u128,
                    block_number: 35u64,
                },
                Transaction {
                    from: debtor2.clone(),
                    wei_amount: 2300u128,
                    block_number: 57u64,
                },
            ];

            subject.more_money_received(&transactions, &InsertUpdateCoreReal);

            (
                subject.single_account_status(&debtor1).unwrap(),
                subject.single_account_status(&debtor2).unwrap(),
            )
        };

        assert_eq!(status1.wallet, debtor1);
        assert_eq!(status1.balance, 34);
        let timestamp1 = dao_utils::to_time_t(status1.last_received_timestamp);
        assert!(timestamp1 >= before);
        assert!(timestamp1 <= dao_utils::to_time_t(SystemTime::now()));

        assert_eq!(status2.wallet, debtor2);
        assert_eq!(status2.balance, 45);
        let timestamp2 = dao_utils::to_time_t(status2.last_received_timestamp);
        assert!(timestamp2 >= before);
        assert!(timestamp2 <= dao_utils::to_time_t(SystemTime::now()));

        let config_dao = ConfigDaoReal::new(
            DbInitializerReal::default()
                .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
                .unwrap(),
        );
        let persistent_config = PersistentConfigurationReal::new(Box::new(config_dao));
        let start_block = persistent_config.start_block().unwrap();
        assert_eq!(57u64, start_block);
    }

    #[test]
    fn more_money_received_throws_away_payments_from_unknown_addresses() {
        let home_dir = ensure_node_home_directory_exists(
            "receivable_dao",
            "more_money_received_throws_away_payments_from_unknown_addresses",
        );
        let debtor = make_wallet("unknown_wallet");
        let mut subject = ReceivableDaoReal::new(
            DbInitializerReal::default()
                .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
                .unwrap(),
        );
        let transactions = vec![Transaction {
                from: debtor.clone(),
                wei_amount: 2300u128,
                block_number: 33u64,
        }];

        let result = subject.more_money_received(&transactions, &InsertUpdateCoreReal);

        assert_eq!(result,Err(AccountantError::ReceivableError(ReceivableError::RusqliteError("Updating balance \
for receivable of -2300 Wei to 0x000000000000756e6b6e6f776e5f77616c6c6574; failing on: 'Query returned no \
rows'; couldn't update an account calling try_multi_insert_payment()".to_string()))));
        let status = subject.single_account_status(&debtor);
        assert!(status.is_none());
    }

    #[test]
    fn receivable_account_status_works_when_account_doesnt_exist() {
        let home_dir = ensure_node_home_directory_exists(
            "receivable_dao",
            "receivable_account_status_works_when_account_doesnt_exist",
        );
        let wallet = make_wallet("booga");
        let subject = ReceivableDaoReal::new(
            DbInitializerReal::default()
                .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
                .unwrap(),
        );

        let result = subject.single_account_status(&wallet);

        assert_eq!(result, None);
    }

    #[test]
    fn receivables_fetches_all_receivable_accounts() {
        let home_dir = ensure_node_home_directory_exists(
            "receivable_dao",
            "receivables_fetches_all_receivable_accounts",
        );
        let wallet1 = make_wallet("wallet1");
        let wallet2 = make_wallet("wallet2");
        let time_stub = SystemTime::now();

        let subject = ReceivableDaoReal::new(
            DbInitializerReal::default()
                .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
                .unwrap(),
        );

        subject
            .more_money_receivable(&wallet1, 1234, &InsertUpdateCoreReal)
            .unwrap();
        subject
            .more_money_receivable(&wallet2, 2345, &InsertUpdateCoreReal)
            .unwrap();

        let accounts = subject
            .all_receivable_accounts()
            .into_iter()
            .map(|r| ReceivableAccount {
                last_received_timestamp: time_stub,
                ..r
            })
            .collect::<Vec<ReceivableAccount>>();

        assert_eq!(
            vec![
                ReceivableAccount {
                    wallet: wallet1,
                    balance: 1234,
                    last_received_timestamp: time_stub
                },
                ReceivableAccount {
                    wallet: wallet2,
                    balance: 2345,
                    last_received_timestamp: time_stub
                },
            ],
            accounts
        )
    }

    #[test]
    fn fetching_potential_new_delinquencies_and_their_curve_heights() {
        //these values are actually irrelevant because the decision
        //is made based on whether the wallet is listed among the current known delinquents;
        //we would judge the quality of the debt in the next step
        let pcs = PaymentCurves {
            payment_suggested_after_sec: 25,
            payment_grace_before_ban_sec: 50,
            permanent_debt_allowed_wei: 100,
            balance_to_decrease_from_wei: 200,
            balance_decreases_for_sec: 100,
            unban_when_balance_below_wei: 0,
        };
        let slope = (pcs.permanent_debt_allowed_wei as f64
            - pcs.balance_to_decrease_from_wei as f64)
            / (pcs.balance_decreases_for_sec as f64);
        let home_dir =
            ensure_node_home_directory_exists("accountant", "fetching_potential_new_delinquencies_and_their_curve_heights");
        let conn = DbInitializerReal::default()
            .initialize(&home_dir,TEST_DEFAULT_CHAIN,true).unwrap();
        let wallet_banned = make_wallet("wallet_banned");
        let wallet_1 = make_wallet("wallet_1");
        let wallet_2 = make_wallet("wallet_2");
        let unbanned_account_1_timestamp = from_time_t(38_400_000_000);
        let unbanned_account_2_timestamp = SystemTime::now();
        let banned_account= ReceivableAccount{
            wallet: wallet_banned,
            balance: 80057,
            last_received_timestamp: from_time_t(26_500_000_000)
        };
        let unbanned_account_1 = ReceivableAccount{
            wallet: wallet_1.clone(),
            balance: 8500,
            last_received_timestamp: unbanned_account_1_timestamp
        };
        let unbanned_account_2 = ReceivableAccount{
            wallet: wallet_2.clone(),
            balance: 30,
            last_received_timestamp:unbanned_account_2_timestamp
        };
        add_receivable_account(&conn,&banned_account);
        add_banned_account(&conn, &banned_account);
        add_receivable_account(&conn,&unbanned_account_1);
        add_receivable_account(&conn,&unbanned_account_2);
        let subject =  ReceivableDaoReal::new(conn);

        subject.create_temporary_table_with_metadata_for_yet_unbanned(&pcs);

        let now = now_time_t();
        let mut captured = capture_rows(subject.conn.as_ref(),"delinquency_metadata");
        let expected_point_height_for_unbanned_1 =
            ReceivableDaoReal::delinquency_curve_height_detection(&pcs,now,to_time_t(unbanned_account_1_timestamp),slope).unwrap();
        let expected_point_height_for_unbanned_2 =
            ReceivableDaoReal::delinquency_curve_height_detection(&pcs,now,to_time_t(unbanned_account_2_timestamp),slope).unwrap();
        assert_eq!(captured,vec![(wallet_1.to_string(), expected_point_height_for_unbanned_1), (wallet_2.to_string(),expected_point_height_for_unbanned_2)]);
    }

    fn capture_rows(conn:&dyn ConnectionWrapper, table:&str)-> Vec<(String,i128)>{
        let mut stm = conn.prepare(&format!("select * from {}",table)).unwrap();

        stm.query_map([],|row|{
            let wallet:String = row.get(0).unwrap();
            let curve_value:i128 = row.get(1).unwrap();
            Ok((wallet,curve_value))
        }).unwrap().flat_map(|val|val).collect::<Vec<(String,i128)>>()
    }

    #[test]
    fn new_delinquencies_unit_slope() {
        let pcs = PaymentCurves {
            payment_suggested_after_sec: 25,
            payment_grace_before_ban_sec: 50,
            permanent_debt_allowed_wei: 100,
            balance_to_decrease_from_wei: 200,
            balance_decreases_for_sec: 100,
            unban_when_balance_below_wei: 0, // doesn't matter for this test
        };
        let now = now_time_t();
        let mut not_delinquent_inside_grace_period = make_receivable_account(1234, false);
        not_delinquent_inside_grace_period.balance =
            u128_to_signed(pcs.balance_to_decrease_from_wei + 1).unwrap();
        not_delinquent_inside_grace_period.last_received_timestamp =
            from_time_t(pcs.sugg_and_grace(now) + 2);
        let mut not_delinquent_after_grace_below_slope = make_receivable_account(2345, false);
        not_delinquent_after_grace_below_slope.balance =
            u128_to_signed(pcs.balance_to_decrease_from_wei - 2).unwrap();
        not_delinquent_after_grace_below_slope.last_received_timestamp =
            from_time_t(pcs.sugg_and_grace(now) - 1);
        let mut delinquent_above_slope_after_grace = make_receivable_account(3456, true);
        delinquent_above_slope_after_grace.balance =
            u128_to_signed(pcs.balance_to_decrease_from_wei - 1).unwrap();
        delinquent_above_slope_after_grace.last_received_timestamp =
            from_time_t(pcs.sugg_and_grace(now) - 2);
        let mut not_delinquent_below_slope_before_stop = make_receivable_account(4567, false);
        not_delinquent_below_slope_before_stop.balance =
            u128_to_signed(pcs.permanent_debt_allowed_wei + 1).unwrap();
        not_delinquent_below_slope_before_stop.last_received_timestamp =
            from_time_t(pcs.sugg_thru_decreasing(now) + 2);
        let mut delinquent_above_slope_before_stop = make_receivable_account(5678, true);
        delinquent_above_slope_before_stop.balance =
            u128_to_signed(pcs.permanent_debt_allowed_wei + 2).unwrap();
        delinquent_above_slope_before_stop.last_received_timestamp =
            from_time_t(pcs.sugg_thru_decreasing(now) + 1);
        let mut not_delinquent_above_slope_after_stop = make_receivable_account(6789, false);
        not_delinquent_above_slope_after_stop.balance =
            u128_to_signed(pcs.permanent_debt_allowed_wei - 1).unwrap();
        not_delinquent_above_slope_after_stop.last_received_timestamp =
            from_time_t(pcs.sugg_thru_decreasing(now) - 2);
        let home_dir = ensure_node_home_directory_exists("accountant", "new_delinquencies");
        let db_initializer = DbInitializerReal::default();
        let conn = db_initializer
            .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
            .unwrap();
        add_receivable_account(&conn, &not_delinquent_inside_grace_period);
        add_receivable_account(&conn, &not_delinquent_after_grace_below_slope);
        add_receivable_account(&conn, &delinquent_above_slope_after_grace);
        add_receivable_account(&conn, &not_delinquent_below_slope_before_stop);
        add_receivable_account(&conn, &delinquent_above_slope_before_stop);
        add_receivable_account(&conn, &not_delinquent_above_slope_after_stop);
        let subject = ReceivableDaoReal::new(conn);

        let result = subject.new_delinquencies(from_time_t(now), &pcs).unwrap();

        assert_contains(&result, &delinquent_above_slope_after_grace);
        assert_contains(&result, &delinquent_above_slope_before_stop);
        assert_eq!(2, result.len());
    }

    #[test]
    fn new_delinquencies_shallow_slope() {
        let pcs = PaymentCurves {
            payment_suggested_after_sec: 100,
            payment_grace_before_ban_sec: 100,
            permanent_debt_allowed_wei: 100,
            balance_to_decrease_from_wei: 110,
            balance_decreases_for_sec: 100,
            unban_when_balance_below_wei: 0, // doesn't matter for this test
        };
        let now = now_time_t();
        let mut not_delinquent = make_receivable_account(1234, false);
        not_delinquent.balance = 105;
        not_delinquent.last_received_timestamp = from_time_t(pcs.sugg_and_grace(now) - 25);
        let mut delinquent = make_receivable_account(2345, true);
        delinquent.balance = 105;
        delinquent.last_received_timestamp = from_time_t(pcs.sugg_and_grace(now) - 75);
        let home_dir =
            ensure_node_home_directory_exists("accountant", "new_delinquencies_shallow_slope");
        let db_initializer = DbInitializerReal::default();
        let conn = db_initializer
            .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
            .unwrap();
        add_receivable_account(&conn, &not_delinquent);
        add_receivable_account(&conn, &delinquent);
        let subject = ReceivableDaoReal::new(conn);

        let result = subject.new_delinquencies(from_time_t(now), &pcs).unwrap();

        assert_contains(&result, &delinquent);
        assert_eq!(1, result.len());
    }

    #[test]
    fn new_delinquencies_steep_slope() {
        let pcs = PaymentCurves {
            payment_suggested_after_sec: 100,
            payment_grace_before_ban_sec: 100,
            permanent_debt_allowed_wei: 100,
            balance_to_decrease_from_wei: 1100,
            balance_decreases_for_sec: 100,
            unban_when_balance_below_wei: 0, // doesn't matter for this test
        };
        let now = now_time_t();
        let mut not_delinquent = make_receivable_account(1234, false);
        not_delinquent.balance = 600;
        not_delinquent.last_received_timestamp = from_time_t(pcs.sugg_and_grace(now) - 25);
        let mut delinquent = make_receivable_account(2345, true);
        delinquent.balance = 600;
        delinquent.last_received_timestamp = from_time_t(pcs.sugg_and_grace(now) - 75);
        let home_dir =
            ensure_node_home_directory_exists("accountant", "new_delinquencies_steep_slope");
        let db_initializer = DbInitializerReal::default();
        let conn = db_initializer
            .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
            .unwrap();
        add_receivable_account(&conn, &not_delinquent);
        add_receivable_account(&conn, &delinquent);
        let subject = ReceivableDaoReal::new(conn);

        let result = subject.new_delinquencies(from_time_t(now), &pcs).unwrap();

        assert_contains(&result, &delinquent);
        assert_eq!(1, result.len());
    }

    #[test]
    fn new_delinquencies_does_not_find_existing_delinquencies() {
        let pcs = PaymentCurves {
            payment_suggested_after_sec: 25,
            payment_grace_before_ban_sec: 50,
            permanent_debt_allowed_wei: 100,
            balance_to_decrease_from_wei: 200,
            balance_decreases_for_sec: 100,
            unban_when_balance_below_wei: 0, // doesn't matter for this test
        };
        let now = now_time_t();
        let mut existing_delinquency = make_receivable_account(1234, true);
        existing_delinquency.balance = 250;
        existing_delinquency.last_received_timestamp = from_time_t(pcs.sugg_and_grace(now) - 1);
        let mut new_delinquency = make_receivable_account(2345, true);
        new_delinquency.balance = 250;
        new_delinquency.last_received_timestamp = from_time_t(pcs.sugg_and_grace(now) - 1);

        let home_dir = ensure_node_home_directory_exists(
            "receivable_dao",
            "new_delinquencies_does_not_find_existing_delinquencies",
        );
        let db_initializer = DbInitializerReal::default();
        let conn = db_initializer
            .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
            .unwrap();
        add_receivable_account(&conn, &existing_delinquency);
        add_receivable_account(&conn, &new_delinquency);
        add_banned_account(&conn, &existing_delinquency);
        let subject = ReceivableDaoReal::new(conn);

        let result = subject.new_delinquencies(from_time_t(now), &pcs).unwrap();

        assert_contains(&result, &new_delinquency);
        assert_eq!(1, result.len());
    }

    #[test]
    fn paid_delinquencies() {
        let pcs = PaymentCurves {
            payment_suggested_after_sec: 0,  // doesn't matter for this test
            payment_grace_before_ban_sec: 0, // doesn't matter for this test
            permanent_debt_allowed_wei: 0,   // doesn't matter for this test
            balance_to_decrease_from_wei: 0, // doesn't matter for this test
            balance_decreases_for_sec: 0,    // doesn't matter for this test
            unban_when_balance_below_wei: 50,
        };
        let mut paid_delinquent = make_receivable_account(1234, true);
        paid_delinquent.balance = 50;
        let mut unpaid_delinquent = make_receivable_account(2345, true);
        unpaid_delinquent.balance = 51;
        let home_dir = ensure_node_home_directory_exists("accountant", "paid_delinquencies");
        let db_initializer = DbInitializerReal::default();
        let conn = db_initializer
            .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
            .unwrap();
        add_receivable_account(&conn, &paid_delinquent);
        add_receivable_account(&conn, &unpaid_delinquent);
        add_banned_account(&conn, &paid_delinquent);
        add_banned_account(&conn, &unpaid_delinquent);
        let subject = ReceivableDaoReal::new(conn);

        let result = subject.paid_delinquencies(&pcs).unwrap();

        assert_contains(&result, &paid_delinquent);
        assert_eq!(1, result.len());
    }

    #[test]
    fn paid_delinquencies_does_not_find_existing_nondelinquencies() {
        let pcs = PaymentCurves {
            payment_suggested_after_sec: 0,  // doesn't matter for this test
            payment_grace_before_ban_sec: 0, // doesn't matter for this test
            permanent_debt_allowed_wei: 0,   // doesn't matter for this test
            balance_to_decrease_from_wei: 0, // doesn't matter for this test
            balance_decreases_for_sec: 0,    // doesn't matter for this test
            unban_when_balance_below_wei: 50,
        };
        let mut newly_non_delinquent = make_receivable_account(1234, false);
        newly_non_delinquent.balance = 25;
        let mut old_non_delinquent = make_receivable_account(2345, false);
        old_non_delinquent.balance = 25;

        let home_dir = ensure_node_home_directory_exists(
            "receivable_dao",
            "paid_delinquencies_does_not_find_existing_nondelinquencies",
        );
        let db_initializer = DbInitializerReal::default();
        let conn = db_initializer
            .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
            .unwrap();
        add_receivable_account(&conn, &newly_non_delinquent);
        add_receivable_account(&conn, &old_non_delinquent);
        add_banned_account(&conn, &newly_non_delinquent);
        let subject = ReceivableDaoReal::new(conn);

        let result = subject.paid_delinquencies(&pcs).unwrap();

        assert_contains(&result, &newly_non_delinquent);
        assert_eq!(1, result.len());
    }

    #[test]
    fn top_records_and_total() {
        let home_dir = ensure_node_home_directory_exists("receivable_dao", "top_records_and_total");
        let conn = DbInitializerReal::default()
            .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
            .unwrap();
        let insert = |wallet: &str, balance: i64, timestamp: i64| {
            let params: &[&dyn ToSql] = &[&wallet, &balance, &timestamp];
            conn
                .prepare("insert into receivable (wallet_address, balance, last_received_timestamp) values (?, ?, ?)")
                .unwrap()
                .execute(params)
                .unwrap();
        };
        let timestamp1 = dao_utils::now_time_t() - 80_000;
        let timestamp2 = dao_utils::now_time_t() - 86_401;
        let timestamp3 = dao_utils::now_time_t() - 86_000;
        let timestamp4 = dao_utils::now_time_t() - 86_001;
        insert(
            "0x1111111111111111111111111111111111111111",
            999_999_999, // below minimum amount - reject
            timestamp1,  // below maximum age
        );
        insert(
            "0x2222222222222222222222222222222222222222",
            1_000_000_000, // minimum amount
            timestamp2,    // above maximum age - reject
        );
        insert(
            "0x3333333333333333333333333333333333333333",
            1_000_000_000, // minimum amount
            timestamp3,    // below maximum age
        );
        insert(
            "0x4444444444444444444444444444444444444444",
            1_000_000_001, // above minimum amount
            timestamp4,    // below maximum age
        );

        let subject = ReceivableDaoReal::new(conn);

        let top_records = subject.top_records(1_000_000_000, 86400).unwrap();
        let total = subject.total();

        assert_eq!(
            top_records,
            vec![
                ReceivableAccount {
                    wallet: Wallet::new("0x4444444444444444444444444444444444444444"),
                    balance: 1_000_000_001,
                    last_received_timestamp: dao_utils::from_time_t(timestamp4),
                },
                ReceivableAccount {
                    wallet: Wallet::new("0x3333333333333333333333333333333333333333"),
                    balance: 1_000_000_000,
                    last_received_timestamp: dao_utils::from_time_t(timestamp3),
                },
            ]
        );
        assert_eq!(total, 4_000_000_000)
    }

    #[test]
    fn correctly_totals_zero_records() {
        let home_dir =
            ensure_node_home_directory_exists("receivable_dao", "correctly_totals_zero_records");
        let conn = DbInitializerReal::default()
            .initialize(&home_dir, TEST_DEFAULT_CHAIN, true)
            .unwrap();
        let subject = ReceivableDaoReal::new(conn);

        let result = subject.total();

        assert_eq!(result, 0)
    }

    fn add_receivable_account(conn: &Box<dyn ConnectionWrapper>, account: &ReceivableAccount) {
        let mut stmt = conn.prepare ("insert into receivable (wallet_address, balance, last_received_timestamp) values (?, ?, ?)").unwrap();
        let params: &[&dyn ToSql] = &[
            &account.wallet,
            &account.balance,
            &to_time_t(account.last_received_timestamp),
        ];
        stmt.execute(params).unwrap();
    }

    fn add_banned_account(conn: &Box<dyn ConnectionWrapper>, account: &ReceivableAccount) {
        let mut stmt = conn
            .prepare("insert into banned (wallet_address) values (?)")
            .unwrap();
        stmt.execute(&[&account.wallet]).unwrap();
    }
}
