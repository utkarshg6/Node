// Copyright (c) 2019, MASQ (https://masq.ai) and/or its affiliates. All rights reserved.

pub(in crate::accountant) mod scanners {
    use crate::accountant::{
        Accountant, CancelFailedPendingTransaction, ConfirmPendingTransaction,
        RequestTransactionReceipts, ResponseSkeleton, ScanForPayables, ScanForPendingPayables,
        ScanForReceivables,
    };
    use crate::sub_lib::utils::{NotifyHandle, NotifyLaterHandle};
    use actix::{Context, Recipient};
    use masq_lib::logger::timestamp_as_string;
    use masq_lib::messages::ScanType;
    use std::cell::RefCell;
    use std::time::SystemTime;

    pub type Scan = Box<dyn Fn(&Accountant, Option<ResponseSkeleton>)>;
    pub type NotifyLaterAssertable = Box<dyn FnMut(&Accountant, &mut Context<Accountant>)>;

    pub struct Scanners {
        pub pending_payables: Scanner,
        pub payables: Scanner,
        pub receivables: Scanner,
    }

    impl Default for Scanners {
        fn default() -> Self {
            Scanners {
                pending_payables: Scanner::new(
                    ScanType::PendingPayables,
                    Box::new(|accountant, response_skeleton_opt| {
                        accountant.scan_for_pending_payable(response_skeleton_opt)
                    }),
                    Box::new(|accountant, ctx| {
                        let _ = accountant
                            .notify_later
                            .scan_for_pending_payable
                            .notify_later(
                                ScanForPendingPayables {
                                    response_skeleton_opt: None, // because scheduled scans don't respond
                                },
                                accountant
                                    .config
                                    .scan_intervals
                                    .pending_payable_scan_interval,
                                ctx,
                            );
                    }),
                ),
                payables: Scanner::new(
                    ScanType::Payables,
                    Box::new(|accountant, response_skeleton_opt| {
                        accountant.scan_for_payables(response_skeleton_opt)
                    }),
                    Box::new(|accountant, ctx| {
                        let _ = accountant.notify_later.scan_for_payable.notify_later(
                            ScanForPayables {
                                response_skeleton_opt: None,
                            },
                            accountant.config.scan_intervals.payable_scan_interval,
                            ctx,
                        );
                    }),
                ),
                receivables: Scanner::new(
                    ScanType::Receivables,
                    Box::new(|accountant, response_skeleton_opt| {
                        // TODO: Figure out how to combine the results of these two into a single response to the UI
                        accountant.scan_for_receivables(response_skeleton_opt);
                        accountant.scan_for_delinquencies();
                    }),
                    Box::new(|accountant, ctx| {
                        let _ = accountant.notify_later.scan_for_receivable.notify_later(
                            ScanForReceivables {
                                response_skeleton_opt: None,
                            },
                            accountant.config.scan_intervals.receivable_scan_interval,
                            ctx,
                        );
                    }),
                ),
            }
        }
    }

    pub struct Scanner {
        scan_type: ScanType,
        initiated_at: Option<SystemTime>,
        scan: Scan,
        notify_later_assertable: RefCell<NotifyLaterAssertable>,
    }

    impl Scanner {
        pub fn new(
            scan_type: ScanType,
            scan: Scan,
            notify_later_assertable: NotifyLaterAssertable,
        ) -> Scanner {
            Scanner {
                scan_type,
                initiated_at: None,
                scan,
                notify_later_assertable: RefCell::new(notify_later_assertable),
            }
        }

        pub fn scan(
            &self,
            accountant: &Accountant,
            response_skeleton_opt: Option<ResponseSkeleton>,
        ) -> Result<(), String> {
            if let Some(initiated_at) = self.initiated_at {
                return Err(format!(
                    "{:?} scan was already initiated at {}. Hence, this scan request will be ignored.",
                    self.scan_type, timestamp_as_string(&initiated_at)
                ));
            };

            (self.scan)(accountant, response_skeleton_opt);
            Ok(())
        }

        pub fn notify_later_assertable(
            &self,
            accountant: &Accountant,
            ctx: &mut Context<Accountant>,
        ) {
            (self.notify_later_assertable.borrow_mut())(accountant, ctx);
        }

        pub fn scan_type(&self) -> ScanType {
            self.scan_type
        }

        pub fn is_scan_running(&self) -> bool {
            self.initiated_at.is_some()
        }

        pub fn mark_as_started(&mut self, timestamp: SystemTime) {
            self.initiated_at = Some(timestamp)
        }
    }

    #[derive(Default)]
    pub struct NotifyLaterForScanners {
        pub scan_for_pending_payable:
            Box<dyn NotifyLaterHandle<ScanForPendingPayables, Accountant>>,
        pub scan_for_payable: Box<dyn NotifyLaterHandle<ScanForPayables, Accountant>>,
        pub scan_for_receivable: Box<dyn NotifyLaterHandle<ScanForReceivables, Accountant>>,
    }

    #[derive(Default)]
    pub struct TransactionConfirmationTools {
        pub notify_confirm_transaction:
            Box<dyn NotifyHandle<ConfirmPendingTransaction, Accountant>>,
        pub notify_cancel_failed_transaction:
            Box<dyn NotifyHandle<CancelFailedPendingTransaction, Accountant>>,
        pub request_transaction_receipts_subs_opt: Option<Recipient<RequestTransactionReceipts>>,
    }
}

#[cfg(test)]
mod tests {
    use crate::accountant::scanners::scanners::Scanners;
    use crate::accountant::test_utils::{bc_from_ac_plus_earning_wallet, AccountantBuilder};
    use crate::test_utils::make_wallet;
    use crate::test_utils::unshared_test_utils::make_populated_accountant_config_with_defaults;
    use masq_lib::logger::timestamp_as_string;
    use masq_lib::messages::ScanType;
    use std::time::SystemTime;

    #[test]
    fn scan_can_be_marked_as_started() {
        let mut subject = Scanners::default();
        let initial_flag = subject.payables.is_scan_running();
        let now = SystemTime::now();

        subject.payables.mark_as_started(now);

        let final_flag = subject.payables.is_scan_running();
        assert_eq!(initial_flag, false);
        assert_eq!(final_flag, true);
    }

    #[test]
    fn scanners_are_defaulted_properly() {
        let subject = Scanners::default();

        assert_eq!(
            subject.pending_payables.scan_type(),
            ScanType::PendingPayables
        );
        assert_eq!(subject.payables.scan_type(), ScanType::Payables);
        assert_eq!(subject.receivables.scan_type(), ScanType::Receivables);
        assert_eq!(subject.pending_payables.is_scan_running(), false);
        assert_eq!(subject.payables.is_scan_running(), false);
        assert_eq!(subject.receivables.is_scan_running(), false);
    }

    #[test]
    fn scan_function_throws_error_in_case_scan_is_already_running() {
        let mut subject = Scanners::default();
        let accountant = AccountantBuilder::default()
            .bootstrapper_config(bc_from_ac_plus_earning_wallet(
                make_populated_accountant_config_with_defaults(),
                make_wallet("some_wallet_address"),
            ))
            .build();
        let now = SystemTime::now();
        subject.pending_payables.mark_as_started(now);

        let result = subject.pending_payables.scan(&accountant, None);

        assert_eq!(
            result,
            Err(format!(
                "PendingPayables scan was already initiated at {}. \
                Hence, this scan request will be ignored.",
                timestamp_as_string(&now)
            ))
        );
    }
}
