// Copyright (c) 2024, MASQ (https://masq.ai) and/or its affiliates. All rights reserved.

#![cfg(test)]

use crate::accountant::db_access_objects::payable_dao::PayableAccount;
use crate::accountant::db_access_objects::utils::{from_time_t, to_time_t};
use crate::accountant::payment_adjuster::miscellaneous::helper_functions::sum_as;
use crate::accountant::payment_adjuster::preparatory_analyser::accounts_abstraction::BalanceProvidingAccount;
use crate::accountant::payment_adjuster::test_utils::PRESERVED_TEST_PAYMENT_THRESHOLDS;
use crate::accountant::payment_adjuster::{
    Adjustment, AdjustmentAnalysis, PaymentAdjuster, PaymentAdjusterError, PaymentAdjusterReal,
};
use crate::accountant::scanners::mid_scan_msg_handling::payable_scanner::test_utils::BlockchainAgentMock;
use crate::accountant::scanners::mid_scan_msg_handling::payable_scanner::PreparedAdjustment;
use crate::accountant::test_utils::try_making_guaranteed_qualified_payables;
use crate::accountant::{gwei_to_wei, AnalyzedPayableAccount};
use crate::sub_lib::blockchain_bridge::OutboundPaymentsInstructions;
use crate::sub_lib::wallet::Wallet;
use crate::test_utils::make_wallet;
use itertools::{Either, Itertools};
use masq_lib::percentage::Percentage;
use masq_lib::test_utils::utils::ensure_node_home_directory_exists;
use masq_lib::utils::convert_collection;
use rand;
use rand::rngs::ThreadRng;
use rand::{thread_rng, Rng};
use std::fs::File;
use std::io::Write;
use std::time::SystemTime;
use thousands::Separable;
use web3::types::U256;

#[test]
//#[ignore]
fn loading_test_with_randomized_params() {
    // This test needs to be understood as a fuzz test, a generator of possibly an overwhelming
    // amount of scenarios that the PaymentAdjuster might happen to be asked to sort them out while
    // there might be many and many combinations that a human would be having a hard time imagining;
    // now we ought to ponder that some of them might be corner cases of which there was no
    // awareness when it was being designed. So, the main purpose of this test is to prove that even
    // a huge number of goes with as-much-as-possible variable inputs will not shoot
    // the PaymentAdjuster down and potentially the whole Node with it; on contrary, it should
    // always give some reasonable results and live up to its original purpose with account
    // adjustments. That said, a smaller amount of these attempts are expected to end up vain,
    // legitimately, though, because of so defined error cases.

    // When the test finishes, it writes some key figures of each pseudo-randomly invented scenario,
    // and a summary of the whole test set with some useful statistics that can help to evaluate
    // the behavior to better understand this feature in the Node or consider some enhancement to be
    // implemented.

    // For somebody not so familiar with the algorithms there might emerge results (they tend to be
    // rare but absolutely valid and wanted - the more their observation can be interesting) that
    // can cause some confusion.

    // The scenario generator is designed to provide as random and as wide variety of situations as
    // possible, but it has plenty of limitations anyway. This is an example of rather un unclear
    // result, where the statistical numbers might seem to have no reason and feel like something is
    // broken. Let's make a hint about these percents of the initial debt coverage, that strike by
    // their perhaps surprising lack of proportionality with that trend of their ascending balances.
    // It cannot be that intuitive, though, because the final balance depends heavily on
    // a disqualification limit. Which usually allows not to pay the entire balance but only such
    // portion that will interestedly suffice for us to stay unbanned. For a huge account,
    // the forgiven part is a little fraction of a whole. Small accounts, on the other hand, if it
    // can be applied (and not have to be disqualified), might be missing a big portion of
    // themselves. This is what the numbers 63% and 94% illustrates, despite the former account
    // comes across as it should take precedence and gain at the expanse of the latter.

    // CW service fee balance: 35,592,800,367,641,272 wei
    // Portion of CW balance used: 100%
    // Maximal txt count due to CW txt fee balance: Unlimited
    // ____________________________________________________________________________________________
    // 1,000,494,243,602,844 wei | 567,037 s | 100 %
    // 1,505,465,842,696,120 wei | 540,217 s | 100 %
    // 1,626,173,874,954,904 wei | 349,872 s | 100 %
    // 20,688,283,645,189,616 wei | 472,661 s | 99 %                         # # # # # # # #
    // 4,735,196,705,072,789 wei | 399,335 s | 96 %                          # # # # # # # #
    // 6,770,347,763,782,591 wei | 245,857 s | 92 %                          # # # # # # # #

    // TODO In the future, consider variable PaymentThresholds. Up today, all accounts are inspected
    // based on the same ones and therefore the disqualification limit is rigid and doesn't allow us
    // to observe largely different scenarios or even mixed ones with something yet not really
    // available: Nodes with arbitrary PaymentThresholds settings

    let now = SystemTime::now();
    let mut gn = thread_rng();
    let mut subject = PaymentAdjusterReal::new();
    let number_of_requested_scenarios = 5000;
    let scenarios = generate_scenarios(&mut gn, now, number_of_requested_scenarios);
    let invalidly_generated_scenarios = number_of_requested_scenarios - scenarios.len();
    let test_overall_output_collector =
        TestOverallOutputCollector::new(invalidly_generated_scenarios);

    struct FirstStageOutput {
        test_overall_output_collector: TestOverallOutputCollector,
        allowed_scenarios: Vec<PreparedAdjustment>,
    }

    let init = FirstStageOutput {
        test_overall_output_collector,
        allowed_scenarios: vec![],
    };
    let first_stage_output = scenarios
        .into_iter()
        .fold(init, |mut output_collector, scenario| {
            // We care only about the service fee balance check, parameters for transaction fee can
            // be worked into the scenarios later.
            let qualified_payables = scenario
                .adjustment_analysis
                .accounts
                .iter()
                .map(|account| account.qualified_as.clone())
                .collect();
            let initial_check_result =
                subject.search_for_indispensable_adjustment(qualified_payables, &*scenario.agent);
            let allowed_scenario_opt = match initial_check_result {
                Ok(check_factual_output) => {
                    match check_factual_output {
                        Either::Left(_) => panic!(
                            "Wrong test setup. This test is designed to generate scenarios with \
                            balances always insufficient in some way!"
                        ),
                        Either::Right(_) => (),
                    };
                    Some(scenario)
                }
                Err(
                    PaymentAdjusterError::NotEnoughServiceFeeBalanceEvenForTheSmallestTransaction {
                        ..
                    },
                ) => {
                    output_collector
                        .test_overall_output_collector
                        .scenarios_denied_before_adjustment_started += 1;
                    None
                }
                _e => Some(scenario),
            };

            match allowed_scenario_opt {
                Some(scenario) => output_collector.allowed_scenarios.push(scenario),
                None => (),
            }

            output_collector
        });

    let second_stage_scenarios = first_stage_output.allowed_scenarios;
    let test_overall_output_collector = first_stage_output.test_overall_output_collector;
    let scenario_adjustment_results = second_stage_scenarios
        .into_iter()
        .map(|prepared_adjustment| {
            let account_infos =
                preserve_account_infos(&prepared_adjustment.adjustment_analysis.accounts, now);
            let required_adjustment = prepared_adjustment.adjustment_analysis.adjustment.clone();
            let cw_service_fee_balance_minor =
                prepared_adjustment.agent.service_fee_balance_minor();

            let payment_adjuster_result = subject.adjust_payments(prepared_adjustment, now);

            administrate_single_scenario_result(
                payment_adjuster_result,
                account_infos,
                required_adjustment,
                cw_service_fee_balance_minor,
            )
        })
        .collect();

    render_results_to_file_and_attempt_basic_assertions(
        scenario_adjustment_results,
        number_of_requested_scenarios,
        test_overall_output_collector,
    )
}

fn generate_scenarios(
    gn: &mut ThreadRng,
    now: SystemTime,
    number_of_scenarios: usize,
) -> Vec<PreparedAdjustment> {
    (0..number_of_scenarios)
        .flat_map(|_| try_making_single_valid_scenario(gn, now))
        .collect()
}

fn try_making_single_valid_scenario(
    gn: &mut ThreadRng,
    now: SystemTime,
) -> Option<PreparedAdjustment> {
    let (cw_service_fee_balance, payables) = make_payables(gn, now);
    let qualified_payables = try_making_guaranteed_qualified_payables(
        payables,
        &PRESERVED_TEST_PAYMENT_THRESHOLDS,
        now,
        false,
    );
    let required_service_fee_total: u128 =
        sum_as(&qualified_payables, |account| account.balance_minor());
    if required_service_fee_total <= cw_service_fee_balance {
        return None;
    }
    let analyzed_accounts: Vec<AnalyzedPayableAccount> = convert_collection(qualified_payables);
    let agent = make_agent(cw_service_fee_balance);
    let adjustment = make_adjustment(gn, analyzed_accounts.len());
    Some(PreparedAdjustment::new(
        Box::new(agent),
        None,
        AdjustmentAnalysis::new(adjustment, analyzed_accounts),
    ))
}

fn make_payable_account(
    idx: usize,
    threshold_limit: u128,
    guarantee_age: u64,
    now: SystemTime,
    gn: &mut ThreadRng,
) -> PayableAccount {
    // Why is this construction so complicated? Well, I wanted to get the test showing partial
    // adjustments where the final accounts can be paid enough but still not all up to their
    // formerly claimed balance. It turned out it is very difficult to achieve that, I couldn't
    // really come up with parameters that would certainly bring this condition. I ended up
    // experimenting and looking for an algorithm that would make the parameters as random as
    // possible because the generator alone is not much good at it. This isn't optimal either,
    // but it allows to observe some of those partial adjustments, however, with a rate of maybe
    // 0.5 % out of those all attempts to create a proper test scenario.
    let wallet = make_wallet(&format!("wallet{}", idx));
    let mut generate_age_segment = || generate_non_zero_usize(gn, (guarantee_age / 2) as usize);
    let debt_age = generate_age_segment() + generate_age_segment() + generate_age_segment();
    let service_fee_balance_minor = {
        let mut generate_u128 = || generate_non_zero_usize(gn, 100) as u128;
        let parameter_a = generate_u128();
        let parameter_b = generate_u128();
        let parameter_c = generate_u128();
        let parameter_d = generate_u128();
        let mut use_variable_exponent = |parameter: u128, up_to: usize| {
            parameter.pow(generate_non_zero_usize(gn, up_to) as u32)
        };
        let a_b_c = use_variable_exponent(parameter_a, 3)
            * use_variable_exponent(parameter_b, 4)
            * use_variable_exponent(parameter_c, 5)
            * parameter_d;
        let addition = (0..6).fold(a_b_c, |so_far, subtrahend| {
            if so_far != a_b_c {
                so_far
            } else {
                if let Some(num) =
                    a_b_c.checked_sub(use_variable_exponent(parameter_c, 6 - subtrahend))
                {
                    num
                } else {
                    so_far
                }
            }
        });

        threshold_limit + addition
    };
    let last_paid_timestamp = from_time_t(to_time_t(now) - debt_age as i64);
    PayableAccount {
        wallet,
        balance_wei: service_fee_balance_minor,
        last_paid_timestamp,
        pending_payable_opt: None,
    }
}

fn make_payables(gn: &mut ThreadRng, now: SystemTime) -> (u128, Vec<PayableAccount>) {
    let accounts_count = generate_non_zero_usize(gn, 25) + 1;
    let threshold_limit =
        gwei_to_wei::<u128, u64>(PRESERVED_TEST_PAYMENT_THRESHOLDS.permanent_debt_allowed_gwei);
    let guarantee_age = PRESERVED_TEST_PAYMENT_THRESHOLDS.maturity_threshold_sec
        + PRESERVED_TEST_PAYMENT_THRESHOLDS.threshold_interval_sec;
    let accounts = (0..accounts_count)
        .map(|idx| make_payable_account(idx, threshold_limit, guarantee_age, now, gn))
        .collect::<Vec<_>>();
    let balance_average = {
        let sum: u128 = sum_as(&accounts, |account| account.balance_wei);
        sum / accounts_count as u128
    };
    let cw_service_fee_balance_minor = {
        let multiplier = 1000;
        let max_pieces = accounts_count * multiplier;
        let number_of_pieces = generate_usize(gn, max_pieces - 2) as u128 + 2;
        balance_average / multiplier as u128 * number_of_pieces
    };
    (cw_service_fee_balance_minor, accounts)
}

fn make_agent(cw_service_fee_balance: u128) -> BlockchainAgentMock {
    BlockchainAgentMock::default()
        // We don't care about this check in this test
        .transaction_fee_balance_minor_result(U256::from(u128::MAX))
        // ...as well as we don't here
        .estimated_transaction_fee_per_transaction_minor_result(1)
        // Used in the entry check
        .service_fee_balance_minor_result(cw_service_fee_balance)
        // For evaluation preparations in the test
        .service_fee_balance_minor_result(cw_service_fee_balance)
        // For PaymentAdjuster itself
        .service_fee_balance_minor_result(cw_service_fee_balance)
        .agreed_transaction_fee_margin_result(Percentage::new(15))
}

fn make_adjustment(gn: &mut ThreadRng, accounts_count: usize) -> Adjustment {
    let also_by_transaction_fee = generate_boolean(gn);
    if also_by_transaction_fee && accounts_count > 2 {
        let affordable_transaction_count =
            u16::try_from(generate_non_zero_usize(gn, accounts_count)).unwrap();
        Adjustment::TransactionFeeInPriority {
            affordable_transaction_count,
        }
    } else {
        Adjustment::ByServiceFee
    }
}

fn administrate_single_scenario_result(
    payment_adjuster_result: Result<OutboundPaymentsInstructions, PaymentAdjusterError>,
    account_infos: Vec<AccountInfo>,
    required_adjustment: Adjustment,
    cw_service_fee_balance_minor: u128,
) -> ScenarioResult {
    let common = CommonScenarioInfo {
        cw_service_fee_balance_minor,
        required_adjustment,
    };
    let reinterpreted_result = match payment_adjuster_result {
        Ok(outbound_payment_instructions) => {
            let mut adjusted_accounts = outbound_payment_instructions.affordable_accounts;
            let portion_of_cw_cumulatively_used_percents = {
                let used_absolute: u128 = sum_as(&adjusted_accounts, |account| account.balance_wei);
                ((100 * used_absolute) / common.cw_service_fee_balance_minor) as u8
            };
            let adjusted_accounts =
                interpretable_adjustment_results(account_infos, &mut adjusted_accounts);
            let (partially_sorted_interpretable_adjustments, were_no_accounts_eliminated) =
                sort_interpretable_adjustments(adjusted_accounts);
            Ok(SuccessfulAdjustment {
                common,
                portion_of_cw_cumulatively_used_percents,
                partially_sorted_interpretable_adjustments,
                were_no_accounts_eliminated,
            })
        }
        Err(adjuster_error) => Err(FailedAdjustment {
            common,
            account_infos,
            adjuster_error,
        }),
    };

    ScenarioResult::new(reinterpreted_result)
}

fn interpretable_adjustment_results(
    account_infos: Vec<AccountInfo>,
    adjusted_accounts: &mut Vec<PayableAccount>,
) -> Vec<InterpretableAdjustmentResult> {
    account_infos
        .into_iter()
        .map(|account_info| {
            prepare_interpretable_adjustment_result(account_info, adjusted_accounts)
        })
        .collect()
}

struct ScenarioResult {
    result: Result<SuccessfulAdjustment, FailedAdjustment>,
}

impl ScenarioResult {
    fn new(result: Result<SuccessfulAdjustment, FailedAdjustment>) -> Self {
        Self { result }
    }
}

struct SuccessfulAdjustment {
    common: CommonScenarioInfo,
    portion_of_cw_cumulatively_used_percents: u8,
    partially_sorted_interpretable_adjustments: Vec<InterpretableAdjustmentResult>,
    were_no_accounts_eliminated: bool,
}

struct FailedAdjustment {
    common: CommonScenarioInfo,
    account_infos: Vec<AccountInfo>,
    adjuster_error: PaymentAdjusterError,
}

fn preserve_account_infos(
    accounts: &[AnalyzedPayableAccount],
    now: SystemTime,
) -> Vec<AccountInfo> {
    accounts
        .iter()
        .map(|account| AccountInfo {
            wallet: account.qualified_as.bare_account.wallet.clone(),
            initially_requested_service_fee_minor: account.qualified_as.bare_account.balance_wei,
            debt_age_s: now
                .duration_since(account.qualified_as.bare_account.last_paid_timestamp)
                .unwrap()
                .as_secs(),
        })
        .collect()
}

fn render_results_to_file_and_attempt_basic_assertions(
    scenario_results: Vec<ScenarioResult>,
    number_of_requested_scenarios: usize,
    overall_output_collector: TestOverallOutputCollector,
) {
    let file_dir = ensure_node_home_directory_exists("payment_adjuster", "tests");
    let mut file = File::create(file_dir.join("loading_test_output.txt")).unwrap();
    introduction(&mut file);
    let test_overall_output_collector =
        scenario_results
            .into_iter()
            .fold(overall_output_collector, |acc, scenario_result| {
                do_final_processing_of_single_scenario(&mut file, acc, scenario_result)
            });
    let total_scenarios_evaluated = test_overall_output_collector
        .scenarios_denied_before_adjustment_started
        + test_overall_output_collector.oks
        + test_overall_output_collector.all_accounts_eliminated
        + test_overall_output_collector.insufficient_service_fee_balance;
    write_brief_test_summary_into_file(
        &mut file,
        &test_overall_output_collector,
        number_of_requested_scenarios,
        total_scenarios_evaluated,
    );
    let total_scenarios_handled_including_invalid_ones =
        total_scenarios_evaluated + test_overall_output_collector.invalidly_generated_scenarios;
    assert_eq!(
        total_scenarios_handled_including_invalid_ones, number_of_requested_scenarios,
        "All handled scenarios including those invalid ones ({}) != requested scenarios count ({})",
        total_scenarios_handled_including_invalid_ones, number_of_requested_scenarios
    );
    // The next assertions depend heavily on the setup for the scenario generator!! It rather
    // indicates how well the setting is so that you can adjust it eventually, to see more relevant
    // results
    let entry_check_pass_rate = 100
        - ((test_overall_output_collector.scenarios_denied_before_adjustment_started * 100)
            / total_scenarios_evaluated);
    let required_pass_rate = 70;
    assert!(
        entry_check_pass_rate >= required_pass_rate,
        "Not at least {}% from {} the scenarios \
    generated for this test allows PaymentAdjuster to continue doing its job and ends too early. \
    Instead only {}%. Setup of the test might be needed",
        required_pass_rate,
        total_scenarios_evaluated,
        entry_check_pass_rate
    );
    let ok_adjustment_percentage = (test_overall_output_collector.oks * 100)
        / (total_scenarios_evaluated
            - test_overall_output_collector.scenarios_denied_before_adjustment_started);
    let required_success_rate = 70;
    assert!(
        ok_adjustment_percentage >= required_success_rate,
        "Not at least {}% from {} adjustment procedures from PaymentAdjuster runs finished with success, only {}%",
        required_success_rate,
        total_scenarios_evaluated,
        ok_adjustment_percentage
    );
}

fn introduction(file: &mut File) {
    write_thick_dividing_line(file);
    write_thick_dividing_line(file);
    file.write(b"A short summary can be found at the tail\n")
        .unwrap();
    write_thick_dividing_line(file);
    write_thick_dividing_line(file)
}

fn write_brief_test_summary_into_file(
    file: &mut File,
    overall_output_collector: &TestOverallOutputCollector,
    number_of_requested_scenarios: usize,
    total_of_scenarios_evaluated: usize,
) {
    write_thick_dividing_line(file);
    file.write_fmt(format_args!(
        "\n\
         Scenarios\n\
         Requested:............................. {}\n\
         Actually evaluated:.................... {}\n\n\
         Successful:............................ {}\n\
         Successes with no accounts eliminated:. {}\n\n\
         Transaction fee / mixed adjustments:... {}\n\
         Bills fulfillment distribution:\n\
         {}\n\n\
         Plain service fee adjustments:......... {}\n\
         Bills fulfillment distribution:\n\
         {}\n\n\
         Unsuccessful\n\
         Caught by the entry check:............. {}\n\
         With 'AllAccountsEliminated':.......... {}\n\
         With late insufficient balance errors:. {}\n\n\
         Legend\n\
         Partially adjusted accounts mark:...... {}",
        number_of_requested_scenarios,
        total_of_scenarios_evaluated,
        overall_output_collector.oks,
        overall_output_collector.with_no_accounts_eliminated,
        overall_output_collector
            .fulfillment_distribution_for_transaction_fee_adjustments
            .total_scenarios(),
        overall_output_collector
            .fulfillment_distribution_for_transaction_fee_adjustments
            .render_in_two_lines(),
        overall_output_collector
            .fulfillment_distribution_for_service_fee_adjustments
            .total_scenarios(),
        overall_output_collector
            .fulfillment_distribution_for_service_fee_adjustments
            .render_in_two_lines(),
        overall_output_collector.scenarios_denied_before_adjustment_started,
        overall_output_collector.all_accounts_eliminated,
        overall_output_collector.insufficient_service_fee_balance,
        NON_EXHAUSTED_ACCOUNT_MARKER
    ))
    .unwrap()
}

fn do_final_processing_of_single_scenario(
    file: &mut File,
    mut test_overall_output: TestOverallOutputCollector,
    scenario: ScenarioResult,
) -> TestOverallOutputCollector {
    match scenario.result {
        Ok(positive) => {
            if positive.were_no_accounts_eliminated {
                test_overall_output.with_no_accounts_eliminated += 1
            }
            if matches!(
                positive.common.required_adjustment,
                Adjustment::TransactionFeeInPriority { .. }
            ) {
                test_overall_output
                    .fulfillment_distribution_for_transaction_fee_adjustments
                    .collected_fulfillment_percentages
                    .push(positive.portion_of_cw_cumulatively_used_percents)
            }
            if positive.common.required_adjustment == Adjustment::ByServiceFee {
                test_overall_output
                    .fulfillment_distribution_for_service_fee_adjustments
                    .collected_fulfillment_percentages
                    .push(positive.portion_of_cw_cumulatively_used_percents)
            }
            render_positive_scenario(file, positive);
            test_overall_output.oks += 1;
            test_overall_output
        }
        Err(negative) => {
            match negative.adjuster_error {
                PaymentAdjusterError::NotEnoughTransactionFeeBalanceForSingleTx { .. } => {
                    panic!("impossible in this kind of test without the tx fee initial check")
                }
                PaymentAdjusterError::NotEnoughServiceFeeBalanceEvenForTheSmallestTransaction {
                    ..
                } => test_overall_output.insufficient_service_fee_balance += 1,
                PaymentAdjusterError::AllAccountsEliminated => {
                    test_overall_output.all_accounts_eliminated += 1
                }
            }
            render_negative_scenario(file, negative);
            test_overall_output
        }
    }
}

fn render_scenario_header(
    file: &mut File,
    cw_service_fee_balance_minor: u128,
    portion_of_cw_used_percents: u8,
    required_adjustment: Adjustment,
) {
    file.write_fmt(format_args!(
        "CW service fee balance: {} wei\n\
         Portion of CW balance used: {}%\n\
         Maximal txt count due to CW txt fee balance: {}\n",
        cw_service_fee_balance_minor.separate_with_commas(),
        portion_of_cw_used_percents,
        resolve_affordable_transaction_count(required_adjustment)
    ))
    .unwrap();
}
fn render_positive_scenario(file: &mut File, result: SuccessfulAdjustment) {
    write_thick_dividing_line(file);
    render_scenario_header(
        file,
        result.common.cw_service_fee_balance_minor,
        result.portion_of_cw_cumulatively_used_percents,
        result.common.required_adjustment,
    );
    write_thin_dividing_line(file);
    let adjusted_accounts = result.partially_sorted_interpretable_adjustments;
    adjusted_accounts.into_iter().for_each(|account| {
        single_account_output(
            file,
            account.initial_balance,
            account.debt_age_s,
            account.bills_coverage_in_percentage_opt,
        )
    })
}

const BALANCE_COLUMN_WIDTH: usize = 30;
const AGE_COLUMN_WIDTH: usize = 7;

fn single_account_output(
    file: &mut File,
    balance_minor: u128,
    age_s: u64,
    bill_coverage_in_percentage_opt: Option<u8>,
) {
    let _ = file
        .write_fmt(format_args!(
            "{:>balance_width$} wei | {:>age_width$} s | {}\n",
            balance_minor.separate_with_commas(),
            age_s.separate_with_commas(),
            resolve_account_ending_status_graphically(bill_coverage_in_percentage_opt),
            balance_width = BALANCE_COLUMN_WIDTH,
            age_width = AGE_COLUMN_WIDTH
        ))
        .unwrap();
}

const NON_EXHAUSTED_ACCOUNT_MARKER: &str = "# # # # # # # #";

fn resolve_account_ending_status_graphically(
    bill_coverage_in_percentage_opt: Option<u8>,
) -> String {
    match bill_coverage_in_percentage_opt {
        Some(percentage) => {
            let highlighting = if percentage != 100 {
                NON_EXHAUSTED_ACCOUNT_MARKER
            } else {
                ""
            };
            format!("{} %{:>shift$}", percentage, highlighting, shift = 40)
        }
        None => "X".to_string(),
    }
}

fn render_negative_scenario(file: &mut File, negative_result: FailedAdjustment) {
    write_thick_dividing_line(file);
    render_scenario_header(
        file,
        negative_result.common.cw_service_fee_balance_minor,
        0,
        negative_result.common.required_adjustment,
    );
    write_thin_dividing_line(file);
    negative_result.account_infos.iter().for_each(|account| {
        single_account_output(
            file,
            account.initially_requested_service_fee_minor,
            account.debt_age_s,
            None,
        )
    });
    write_thin_dividing_line(file);
    write_error(file, negative_result.adjuster_error)
}

fn write_error(file: &mut File, error: PaymentAdjusterError) {
    file.write_fmt(format_args!(
        "Scenario resulted in a failure: {:?}\n",
        error
    ))
    .unwrap()
}

fn resolve_affordable_transaction_count(adjustment: Adjustment) -> String {
    match adjustment {
        Adjustment::ByServiceFee => "Unlimited".to_string(),
        Adjustment::TransactionFeeInPriority {
            affordable_transaction_count,
        } => affordable_transaction_count.to_string(),
    }
}

fn write_thick_dividing_line(file: &mut File) {
    write_ln_made_of(file, '=')
}

fn write_thin_dividing_line(file: &mut File) {
    write_ln_made_of(file, '_')
}

fn write_ln_made_of(file: &mut File, char: char) {
    let _ = file
        .write_fmt(format_args!("{}\n", char.to_string().repeat(100)))
        .unwrap();
}

fn prepare_interpretable_adjustment_result(
    account_info: AccountInfo,
    resulted_affordable_accounts: &mut Vec<PayableAccount>,
) -> InterpretableAdjustmentResult {
    let adjusted_account_idx_opt = resulted_affordable_accounts
        .iter()
        .position(|account| account.wallet == account_info.wallet);
    let bills_coverage_in_percentage_opt = match adjusted_account_idx_opt {
        Some(idx) => {
            let adjusted_account = resulted_affordable_accounts.remove(idx);
            assert_eq!(adjusted_account.wallet, account_info.wallet);
            let bill_coverage_in_percentage = {
                let percentage = (adjusted_account.balance_wei * 100)
                    / account_info.initially_requested_service_fee_minor;
                u8::try_from(percentage).unwrap()
            };
            Some(bill_coverage_in_percentage)
        }
        None => None,
    };
    InterpretableAdjustmentResult {
        initial_balance: account_info.initially_requested_service_fee_minor,
        debt_age_s: account_info.debt_age_s,
        bills_coverage_in_percentage_opt,
    }
}

fn sort_interpretable_adjustments(
    interpretable_adjustments: Vec<InterpretableAdjustmentResult>,
) -> (Vec<InterpretableAdjustmentResult>, bool) {
    let (finished, eliminated): (
        Vec<InterpretableAdjustmentResult>,
        Vec<InterpretableAdjustmentResult>,
    ) = interpretable_adjustments
        .into_iter()
        .partition(|adjustment| adjustment.bills_coverage_in_percentage_opt.is_some());
    let were_no_accounts_eliminated = eliminated.is_empty();
    let finished_sorted = finished.into_iter().sorted_by(|result_a, result_b| {
        Ord::cmp(
            &(
                result_b.bills_coverage_in_percentage_opt,
                result_a.initial_balance,
            ),
            &(
                result_a.bills_coverage_in_percentage_opt,
                result_b.initial_balance,
            ),
        )
    });
    let eliminated_sorted = eliminated.into_iter().sorted_by(|result_a, result_b| {
        Ord::cmp(&result_a.initial_balance, &result_b.initial_balance)
    });
    let all_results = finished_sorted.chain(eliminated_sorted).collect();
    (all_results, were_no_accounts_eliminated)
}

fn generate_usize_guts(gn: &mut ThreadRng, low: usize, up_to: usize) -> usize {
    gn.gen_range(low..up_to)
}

fn generate_non_zero_usize(gn: &mut ThreadRng, up_to: usize) -> usize {
    generate_usize_guts(gn, 1, up_to)
}

fn generate_usize(gn: &mut ThreadRng, up_to: usize) -> usize {
    generate_usize_guts(gn, 0, up_to)
}

fn generate_boolean(gn: &mut ThreadRng) -> bool {
    gn.gen()
}

struct TestOverallOutputCollector {
    invalidly_generated_scenarios: usize,
    // First stage: entry check
    // ____________________________________
    scenarios_denied_before_adjustment_started: usize,
    // Second stage: proper adjustments
    // ____________________________________
    oks: usize,
    with_no_accounts_eliminated: usize,
    fulfillment_distribution_for_transaction_fee_adjustments: PercentageFulfillmentDistribution,
    fulfillment_distribution_for_service_fee_adjustments: PercentageFulfillmentDistribution,
    // Errors
    all_accounts_eliminated: usize,
    insufficient_service_fee_balance: usize,
}

impl TestOverallOutputCollector {
    fn new(invalidly_generated_scenarios: usize) -> Self {
        Self {
            invalidly_generated_scenarios,
            scenarios_denied_before_adjustment_started: 0,
            oks: 0,
            with_no_accounts_eliminated: 0,
            fulfillment_distribution_for_transaction_fee_adjustments: Default::default(),
            fulfillment_distribution_for_service_fee_adjustments: Default::default(),
            all_accounts_eliminated: 0,
            insufficient_service_fee_balance: 0,
        }
    }
}

#[derive(Default)]
struct PercentageFulfillmentDistribution {
    collected_fulfillment_percentages: Vec<u8>,
}

impl PercentageFulfillmentDistribution {
    fn render_in_two_lines(&self) -> String {
        #[derive(Default)]
        struct Ranges {
            from_0_to_10: usize,
            from_10_to_20: usize,
            from_20_to_30: usize,
            from_30_to_40: usize,
            from_40_to_50: usize,
            from_50_to_60: usize,
            from_60_to_70: usize,
            from_70_to_80: usize,
            from_80_to_90: usize,
            from_90_to_100: usize,
        }

        let full_count = self.collected_fulfillment_percentages.len();
        let ranges_populated = self.collected_fulfillment_percentages.iter().fold(
            Ranges::default(),
            |mut ranges, current| {
                match current {
                    0..=9 => ranges.from_0_to_10 += 1,
                    10..=19 => ranges.from_10_to_20 += 1,
                    20..=29 => ranges.from_20_to_30 += 1,
                    30..=39 => ranges.from_30_to_40 += 1,
                    40..=49 => ranges.from_40_to_50 += 1,
                    50..=59 => ranges.from_50_to_60 += 1,
                    60..=69 => ranges.from_60_to_70 += 1,
                    70..=79 => ranges.from_70_to_80 += 1,
                    80..=89 => ranges.from_80_to_90 += 1,
                    90..=100 => ranges.from_90_to_100 += 1,
                    _ => panic!("Shouldn't happen"),
                }
                ranges
            },
        );
        let digits = 6.max(full_count.to_string().len());
        format!(
            "Percentage ranges\n\
        {:^digits$}|{:^digits$}|{:^digits$}|{:^digits$}|{:^digits$}|\
        {:^digits$}|{:^digits$}|{:^digits$}|{:^digits$}|{:^digits$}\n\
        {:^digits$}|{:^digits$}|{:^digits$}|{:^digits$}|{:^digits$}|\
        {:^digits$}|{:^digits$}|{:^digits$}|{:^digits$}|{:^digits$}",
            "0-9",
            "10-19",
            "20-29",
            "30-39",
            "40-49",
            "50-59",
            "60-69",
            "70-79",
            "80-89",
            "90-100",
            ranges_populated.from_0_to_10,
            ranges_populated.from_10_to_20,
            ranges_populated.from_20_to_30,
            ranges_populated.from_30_to_40,
            ranges_populated.from_40_to_50,
            ranges_populated.from_50_to_60,
            ranges_populated.from_60_to_70,
            ranges_populated.from_70_to_80,
            ranges_populated.from_80_to_90,
            ranges_populated.from_90_to_100
        )
    }

    fn total_scenarios(&self) -> usize {
        self.collected_fulfillment_percentages.len()
    }
}

struct CommonScenarioInfo {
    cw_service_fee_balance_minor: u128,
    required_adjustment: Adjustment,
}
struct InterpretableAdjustmentResult {
    initial_balance: u128,
    debt_age_s: u64,
    // Account was eliminated from payment if None
    bills_coverage_in_percentage_opt: Option<u8>,
}

struct AccountInfo {
    wallet: Wallet,
    initially_requested_service_fee_minor: u128,
    debt_age_s: u64,
}
