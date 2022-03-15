// Copyright (c) 2019, MASQ (https://masq.ai) and/or its affiliates. All rights reserved.
use crate::messages::{SerializableLogLevel, ToMessageBody, UiLogBroadcast};
use crate::ui_gateway::{MessageTarget, NodeToUiMessage};
use actix::Recipient;
use lazy_static::lazy_static;
use log::logger;
use log::Level;
#[allow(unused_imports)]
use log::Metadata;
#[allow(unused_imports)]
use log::Record;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, Once};

pub static mut LOG_RECIPIENT: LogRecipient = LogRecipient {
    initiated: AtomicBool::new(false),
    recipient_opt: None,
};

lazy_static! {
    pub static ref LOG_RECIPIENT_MUTEX: LogRecipientMutex = LogRecipientMutex {
        initiated: AtomicBool::new(false),
        recipient_opt: Mutex::new(None),
    };
}

const UI_MESSAGE_LOG_LEVEL: Level = Level::Info;

#[cfg(test)]
lazy_static! {
    static ref TEST_LOG_RECIPIENT_GUARD: Mutex<()> = Mutex::new(());
}

pub struct LogRecipient {
    initiated: AtomicBool,
    recipient_opt: Option<Recipient<NodeToUiMessage>>,
}

impl LogRecipient {
    pub fn prepare_log_recipient(recipient: Recipient<NodeToUiMessage>) {
        // loop {
        //     if LOG_RECIPIENT.guard.swap(false) {
        //         LOG_RECIPIENT.recipient_opt = Some (recipient);
        //         LOG_RECIPIENT.guard.swap(true);
        //         break;
        //     }
        //     else {
        //         thread.sleep (Duration::from_millis (10)) // wait for the logger to finish
        //     }
        // }
    }

    fn transmit_log(&self, msg: String, log_level: SerializableLogLevel) {
        if unsafe { LOG_RECIPIENT.initiated.load(Ordering::Relaxed) } {
            todo!("send the log")
        }
        // else {
        //     if unsafe {
        //         LOG_RECIPIENT
        //             .initiation_guard
        //             .swap(false, Ordering::Relaxed)
        //     } {
        //         if let Some(recipient) = unsafe { LOG_RECIPIENT.recipient_opt.as_ref() } {
        //             todo!()
        //         }
        //         //unsafe LOG_RECIPIENT.guard.swap(true)
        //     }
        // }
    }
}

pub struct LogRecipientMutex {
    initiated: AtomicBool,
    recipient_opt: Mutex<Option<Recipient<NodeToUiMessage>>>,
}

// Data structure:
//
// pub struct LogRecipient {
//     guard: AtomicBool,
//     recipient_opt: Option<Recipient<blah>>,
// }
//
// Data item:
//
// static LOG_RECIPIENT: LogRecipient = LogRecipient {guard: AtomicBool::new(false), recipient_opt: None};
//
// Log transmitter:
//
// fn transmit_log (parameters) {
//     if (LOG_RECIPIENT.guard.swap(false)) {
//         // Transmit log to UIGateway if recipient_opt is populated
//         LOG_RECIPIENT.guard.swap(true)
//     }
// }
//
// ActorSystemFactory or delegate:
//
// fn prepare_log_recipient (recipient: Recipient<blah>) {
//     loop {
//         if LOG_RECIPIENT.guard.swap(false) {
//             LOG_RECIPIENT.recipient_opt = Some (recipient);
//             LOG_RECIPIENT.guard.swap(true);
//             break;
//         }
//         else {
//             thread.sleep (Duration::from_millis (10)) // wait for the logger to finish
//         }
//     }
// }

#[derive(Clone)]
pub struct Logger {
    name: String,
    #[cfg(not(feature = "no_test_share"))]
    level_limit: Level,
}

#[macro_export]
macro_rules! trace {
    ($logger: expr, $($arg:tt)*) => {
        $logger.trace(|| format!($($arg)*))
    };
}

#[macro_export]
macro_rules! debug {
    ($logger: expr, $($arg:tt)*) => {
        $logger.debug(|| format!($($arg)*))
    };
}

#[macro_export]
macro_rules! info {
    ($logger: expr, $($arg:tt)*) => {
        $logger.info(|| format!($($arg)*))
    };
}

#[macro_export]
macro_rules! warning {
    ($logger: expr, $($arg:tt)*) => {
        $logger.warning(|| format!($($arg)*))
    };
}

#[macro_export]
macro_rules! error {
    ($logger: expr, $($arg:tt)*) => {
        $logger.error(|| format!($($arg)*))
    };
}

#[macro_export]
macro_rules! fatal {
    ($logger: expr, $($arg:tt)*) => {
        $logger.fatal(|| format!($($arg)*))
    };
}

impl Logger {
    pub fn new(name: &str) -> Logger {
        Logger {
            name: String::from(name),
            #[cfg(not(feature = "no_test_share"))]
            level_limit: Level::Trace,
        }
    }

    pub fn trace<F>(&self, log_function: F)
    where
        F: FnOnce() -> String,
    {
        self.generic_log(Level::Trace, log_function);
    }

    pub fn debug<F>(&self, log_function: F)
    where
        F: FnOnce() -> String,
    {
        self.generic_log(Level::Debug, log_function);
    }

    pub fn info<F>(&self, log_function: F)
    where
        F: FnOnce() -> String,
    {
        self.generic_log(Level::Info, log_function);
    }

    pub fn warning<F>(&self, log_function: F)
    where
        F: FnOnce() -> String,
    {
        self.generic_log(Level::Warn, log_function);
    }

    pub fn error<F>(&self, log_function: F)
    where
        F: FnOnce() -> String,
    {
        self.generic_log(Level::Error, log_function);
    }

    pub fn fatal<F>(&self, log_function: F) -> !
    where
        F: FnOnce() -> String,
    {
        let msg = log_function();
        self.log(Level::Error, msg.clone());
        panic!("{}", msg);
    }

    pub fn trace_enabled(&self) -> bool {
        self.level_enabled(Level::Trace)
    }

    pub fn debug_enabled(&self) -> bool {
        self.level_enabled(Level::Debug)
    }

    pub fn info_enabled(&self) -> bool {
        self.level_enabled(Level::Info)
    }

    pub fn warning_enabled(&self) -> bool {
        self.level_enabled(Level::Warn)
    }

    pub fn error_enabled(&self) -> bool {
        self.level_enabled(Level::Error)
    }

    fn generic_log<F>(&self, level: Level, log_function: F)
    where
        F: FnOnce() -> String,
    {
        match (!UI_MESSAGE_LOG_LEVEL.le(&level), self.level_enabled(level)) {
            // We want to transmit log if - !UI_MESSAGE_LOG_LEVEL.le(&level)
            // We want to save into log file if - self.level_enabled(level)
            (true, true) => {
                todo!("")
                // let msg = log_function();
                // self.transmit(msg.clone(), level.into());
                // self.log(level, msg);
            }
            (true, false) => {
                todo!("")
                // self.transmit(log_function(), level.into())
            }
            (false, true) => {
                todo!("")
                // self.log(level, log_function())
            }
            _ => {
                todo!("")
                // return;
            }
        }
    }

    pub fn log(&self, level: Level, msg: String) {
        logger().log(
            &Record::builder()
                .args(format_args!("{}", msg))
                .module_path(Some(&self.name))
                .level(level)
                .build(),
        );
    }
}

impl From<Level> for SerializableLogLevel {
    fn from(native_level: Level) -> Self {
        // todo!("Test Converison: This needs a seperate test")
        match native_level {
            Level::Error => SerializableLogLevel::Error,
            Level::Warn => SerializableLogLevel::Warn,
            Level::Info => SerializableLogLevel::Info,
            // We shouldn't be doing this
            _ => panic!("should not be needed"),
        }
    }
}

#[cfg(feature = "no_test_share")]
impl Logger {
    pub fn level_enabled(&self, level: Level) -> bool {
        logger().enabled(&Metadata::builder().level(level).target(&self.name).build())
    }
}

#[cfg(not(feature = "no_test_share"))]
impl Logger {
    pub fn level_enabled(&self, level: Level) -> bool {
        level <= self.level_limit
    }

    pub fn set_level_for_a_test(&mut self, level: Level) {
        self.level_limit = level
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::logging::init_test_logging;
    use crate::test_utils::logging::TestLogHandler;
    use crate::ui_gateway::{MessageBody, MessagePath, NodeFromUiMessage};
    use actix::{Actor, AsyncContext, Context, Handler, Message, System};
    use chrono::format::StrftimeItems;
    use chrono::{DateTime, Local};
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::{Arc, Barrier, Mutex};
    use std::thread;
    use std::thread::ThreadId;
    use std::time::{Duration, SystemTime};

    // 1) Send a message to Recorder
    // 2) Assert that testing actor received our message (proof that we called transmit method)
    // 3) Not going to print the logs to logfile
    struct TestUiGateway {
        msg_container: Arc<Mutex<Vec<NodeToUiMessage>>>,
        expected_msg_count: usize,
    }

    impl TestUiGateway {
        fn new(msg_count: usize, received_msgs_arc: &Arc<Mutex<Vec<NodeToUiMessage>>>) -> Self {
            Self {
                msg_container: received_msgs_arc.clone(),
                expected_msg_count: msg_count,
            }
        }
    }

    impl Actor for TestUiGateway {
        type Context = Context<Self>;
    }

    impl Handler<NodeToUiMessage> for TestUiGateway {
        type Result = ();

        fn handle(&mut self, msg: NodeToUiMessage, ctx: &mut Self::Context) -> Self::Result {
            let mut inner = self.msg_container.lock().unwrap();
            inner.push(msg);
            if inner.len() == self.expected_msg_count {
                System::current().stop();
            }
        }
    }

    #[derive(Message)]
    struct ScheduleStop {
        timeout: Duration,
    }

    #[derive(Message)]
    struct Stop {}

    impl Handler<ScheduleStop> for TestUiGateway {
        type Result = ();

        fn handle(&mut self, msg: ScheduleStop, ctx: &mut Self::Context) -> Self::Result {
            ctx.notify_later(Stop {}, msg.timeout);
        }
    }

    impl Handler<Stop> for TestUiGateway {
        type Result = ();

        fn handle(&mut self, msg: Stop, ctx: &mut Self::Context) -> Self::Result {
            System::current().stop()
        }
    }

    #[test]
    fn transmit_log_sends_msg_from_multiple_threads_when_plain_log_recipient_initiated() {
        let _test_guard = &TEST_LOG_RECIPIENT_GUARD.lock().unwrap();
        let number_of_calls = 250;
        let received_msg_arc = Arc::new(Mutex::new(vec![]));
        let ui_gateway = TestUiGateway::new(number_of_calls, &received_msg_arc);
        let addr = ui_gateway.start();
        let recipient = addr.clone().recipient();
        unsafe {
            LOG_RECIPIENT.recipient_opt.replace(recipient);
            LOG_RECIPIENT.initiated.store(true, Ordering::Relaxed)
        }
        let system = System::new("test_system");
        addr.try_send(ScheduleStop {
            timeout: Duration::from_secs(50),
        })
        .unwrap();

        addr.try_send(SendAllAtOncePlain { number_of_calls })
            .unwrap();

        let before = SystemTime::now();
        system.run();
        let after = SystemTime::now();
        let recording = received_msg_arc.lock().unwrap();
        assert_eq!(recording.len(), number_of_calls);
        let mut vec_of_results = vec![];
        recording
            .iter()
            .for_each(|msg| vec_of_results.push(msg.body.payload.as_ref().unwrap_err().0));
        vec_of_results.sort();
        assert_eq!(
            vec_of_results,
            (0..number_of_calls as u64).collect::<Vec<u64>>()
        );
        eprintln!(
            "Time elapsed in between: {}ms",
            after.duration_since(before).unwrap().as_millis()
        )
    }

    #[test]
    fn transmit_log_sends_msg_from_multiple_threads_with_barrier_when_plain_log_recipient_initiated(
    ) {
        let _test_guard = &TEST_LOG_RECIPIENT_GUARD.lock().unwrap();
        let number_of_calls = 250;
        let received_msg_arc = Arc::new(Mutex::new(vec![]));
        let ui_gateway = TestUiGateway::new(number_of_calls, &received_msg_arc);
        let addr = ui_gateway.start();
        let recipient = addr.clone().recipient();
        unsafe {
            LOG_RECIPIENT.recipient_opt.replace(recipient);
            LOG_RECIPIENT.initiated.store(true, Ordering::Relaxed)
        }
        let system = System::new("test_system");
        addr.try_send(ScheduleStop {
            timeout: Duration::from_secs(50),
        })
        .unwrap();

        addr.try_send(SendAllAtOncePlainBarrier { number_of_calls })
            .unwrap();

        let before = SystemTime::now();
        system.run();
        let after = SystemTime::now();
        let recording = received_msg_arc.lock().unwrap();
        assert_eq!(recording.len(), number_of_calls);
        let mut vec_of_results = vec![];
        recording
            .iter()
            .for_each(|msg| vec_of_results.push(msg.body.payload.as_ref().unwrap_err().0));
        vec_of_results.sort();
        assert_eq!(
            vec_of_results,
            (0..number_of_calls as u64).collect::<Vec<u64>>()
        );
        eprintln!(
            "Time elapsed in between: {}ms",
            after.duration_since(before).unwrap().as_millis()
        )
    }

    #[test]
    fn transmit_log_sends_msg_from_multiple_threads_when_mutex_log_recipient_initiated() {
        let _test_guard = &TEST_LOG_RECIPIENT_GUARD.lock().unwrap();
        let number_of_calls = 250;
        let received_msg_arc = Arc::new(Mutex::new(vec![]));
        let ui_gateway = TestUiGateway::new(number_of_calls, &received_msg_arc);
        let addr = ui_gateway.start();
        let recipient = addr.clone().recipient();
        unsafe {
            LOG_RECIPIENT_MUTEX
                .recipient_opt
                .lock()
                .unwrap()
                .replace(recipient);
            LOG_RECIPIENT_MUTEX.initiated.store(true, Ordering::Relaxed)
        }
        let system = System::new("test_system");
        addr.try_send(ScheduleStop {
            timeout: Duration::from_secs(50),
        })
        .unwrap();

        addr.try_send(SendAllAtOnceMutex { number_of_calls })
            .unwrap();

        let before = SystemTime::now();
        system.run();
        let after = SystemTime::now();
        let recording = received_msg_arc.lock().unwrap();
        assert_eq!(recording.len(), number_of_calls);
        let mut vec_of_results = vec![];
        recording
            .iter()
            .for_each(|msg| vec_of_results.push(msg.body.payload.as_ref().unwrap_err().0));
        vec_of_results.sort();
        assert_eq!(
            vec_of_results,
            (0..number_of_calls as u64).collect::<Vec<u64>>()
        );
        eprintln!(
            "Time elapsed in between: {}ms",
            after.duration_since(before).unwrap().as_millis()
        )
    }

    #[test]
    fn transmit_log_sends_msg_from_multiple_threads_with_barrier_when_mutex_log_recipient_initiated(
    ) {
        let _test_guard = &TEST_LOG_RECIPIENT_GUARD.lock().unwrap();
        let number_of_calls = 250;
        let received_msg_arc = Arc::new(Mutex::new(vec![]));
        let ui_gateway = TestUiGateway::new(number_of_calls, &received_msg_arc);
        let addr = ui_gateway.start();
        let recipient = addr.clone().recipient();
        unsafe {
            LOG_RECIPIENT_MUTEX
                .recipient_opt
                .lock()
                .unwrap()
                .replace(recipient);
            LOG_RECIPIENT_MUTEX.initiated.store(true, Ordering::Relaxed)
        }
        let system = System::new("test_system");
        addr.try_send(ScheduleStop {
            timeout: Duration::from_secs(50),
        })
        .unwrap();

        addr.try_send(SendAllAtOnceMutexBarrier { number_of_calls })
            .unwrap();

        let before = SystemTime::now();
        system.run();
        let after = SystemTime::now();
        let recording = received_msg_arc.lock().unwrap();
        assert_eq!(recording.len(), number_of_calls);
        let mut vec_of_results = vec![];
        recording
            .iter()
            .for_each(|msg| vec_of_results.push(msg.body.payload.as_ref().unwrap_err().0));
        vec_of_results.sort();
        assert_eq!(
            vec_of_results,
            (0..number_of_calls as u64).collect::<Vec<u64>>()
        );
        eprintln!(
            "Time elapsed in between: {}ms",
            after.duration_since(before).unwrap().as_millis()
        )
    }

    fn send_message_to_mutex_recipient(num: usize) {
        let mutex_guard = LOG_RECIPIENT_MUTEX.recipient_opt.lock().unwrap();
        eprintln!("Acquired Mutex for '{}'.", num);
        let recipient = unsafe { mutex_guard.as_ref().unwrap() };
        eprintln!("Sending for '{}'.", num);
        recipient.try_send(create_msg(num)).unwrap();
        eprintln!("'{}' was sent.", num);
    }

    fn send_message_to_plain_recipient(num: usize) {
        eprintln!("Acquired plain recipient for '{}'.", num);
        let recipient = unsafe { LOG_RECIPIENT.recipient_opt.as_ref().unwrap() };
        eprintln!("Sending for plain '{}'.", num);
        recipient.try_send(create_msg(num)).unwrap();
        eprintln!("Plain '{}' was sent.", num);
    }

    fn create_msg(num: usize) -> NodeToUiMessage {
        NodeToUiMessage {
            target: MessageTarget::AllClients,
            body: MessageBody {
                opcode: "whatever".to_string(),
                path: MessagePath::FireAndForget,
                payload: Err((num as u64, String::new())),
            },
        }
    }

    #[derive(Message)]
    struct SendAllAtOncePlainBarrier {
        number_of_calls: usize,
    }

    impl Handler<SendAllAtOncePlainBarrier> for TestUiGateway {
        type Result = ();

        fn handle(
            &mut self,
            msg: SendAllAtOncePlainBarrier,
            ctx: &mut Self::Context,
        ) -> Self::Result {
            ctx.set_mailbox_capacity(0);
            let barrier_arc = Arc::new(Barrier::new(msg.number_of_calls));
            let mut join_handle_vector = Vec::new();
            (0..msg.number_of_calls).for_each(|num| {
                let barrier_arc_clone = Arc::clone(&barrier_arc);
                join_handle_vector.push(thread::spawn(move || {
                    barrier_arc_clone.wait();
                    send_message_to_plain_recipient(num)
                }))
            });
        }
    }

    #[derive(Message)]
    struct SendAllAtOncePlain {
        number_of_calls: usize,
    }

    impl Handler<SendAllAtOncePlain> for TestUiGateway {
        type Result = ();

        fn handle(&mut self, msg: SendAllAtOncePlain, ctx: &mut Self::Context) -> Self::Result {
            ctx.set_mailbox_capacity(0);
            let mut join_handle_vector = Vec::new();
            (0..msg.number_of_calls).for_each(|num| {
                join_handle_vector.push(thread::spawn(move || send_message_to_plain_recipient(num)))
            });
        }
    }

    #[derive(Message)]
    struct SendAllAtOnceMutex {
        number_of_calls: usize,
    }

    impl Handler<SendAllAtOnceMutex> for TestUiGateway {
        type Result = ();

        fn handle(&mut self, msg: SendAllAtOnceMutex, ctx: &mut Self::Context) -> Self::Result {
            ctx.set_mailbox_capacity(0);
            let mut join_handle_vector = Vec::new();
            (0..msg.number_of_calls).for_each(|num| {
                join_handle_vector.push(thread::spawn(move || send_message_to_mutex_recipient(num)))
            });
        }
    }

    #[derive(Message)]
    struct SendAllAtOnceMutexBarrier {
        number_of_calls: usize,
    }

    impl Handler<SendAllAtOnceMutexBarrier> for TestUiGateway {
        type Result = ();

        fn handle(
            &mut self,
            msg: SendAllAtOnceMutexBarrier,
            ctx: &mut Self::Context,
        ) -> Self::Result {
            ctx.set_mailbox_capacity(0);
            let barrier_arc = Arc::new(Barrier::new(msg.number_of_calls));
            let mut join_handle_vector = Vec::new();
            (0..msg.number_of_calls).for_each(|num| {
                let barrier_arc_clone = Arc::clone(&barrier_arc);
                join_handle_vector.push(thread::spawn(move || {
                    barrier_arc_clone.wait();
                    send_message_to_mutex_recipient(num)
                }))
            });
        }
    }

    #[test]
    fn logger_format_is_correct() {
        init_test_logging();
        let one_logger = Logger::new("logger_format_is_correct_one");
        let another_logger = Logger::new("logger_format_is_correct_another");

        let before = SystemTime::now();
        error!(one_logger, "one log");
        error!(another_logger, "another log");
        let after = SystemTime::now();

        let tlh = TestLogHandler::new();
        let prefix_len = "0000-00-00T00:00:00.000".len();
        let thread_id = thread::current().id();
        let one_log = tlh.get_log_at(tlh.exists_log_containing(&format!(
            " Thd{}: ERROR: logger_format_is_correct_one: one log",
            thread_id_as_string(thread_id)
        )));
        let another_log = tlh.get_log_at(tlh.exists_log_containing(&format!(
            " Thd{}: ERROR: logger_format_is_correct_another: another log",
            thread_id_as_string(thread_id)
        )));
        let before_str = timestamp_as_string(&before);
        let after_str = timestamp_as_string(&after);
        assert_between(&one_log[..prefix_len], &before_str, &after_str);
        assert_between(&another_log[..prefix_len], &before_str, &after_str);
    }

    #[test]
    fn trace_is_not_computed_when_log_level_is_debug() {
        let logger = make_logger_at_level(Level::Debug);
        let signal = Arc::new(Mutex::new(Some(false)));
        let signal_c = signal.clone();

        let log_function = move || {
            let mut locked_signal = signal_c.lock().unwrap();
            locked_signal.replace(true);
            "blah".to_string()
        };

        logger.trace(log_function);

        assert_eq!(signal.lock().unwrap().as_ref(), Some(&false));
    }

    #[test]
    fn debug_is_not_computed_when_log_level_is_info() {
        let logger = make_logger_at_level(Level::Info);
        let signal = Arc::new(Mutex::new(Some(false)));
        let signal_c = signal.clone();

        let log_function = move || {
            let mut locked_signal = signal_c.lock().unwrap();
            locked_signal.replace(true);
            "blah".to_string()
        };

        logger.debug(log_function);

        assert_eq!(signal.lock().unwrap().as_ref(), Some(&false));
    }

    #[test]
    fn info_is_not_computed_when_log_level_is_warn() {
        let logger = make_logger_at_level(Level::Warn);
        let signal = Arc::new(Mutex::new(Some(false)));
        let signal_c = signal.clone();

        let log_function = move || {
            let mut locked_signal = signal_c.lock().unwrap();
            locked_signal.replace(true);
            "blah".to_string()
        };

        logger.info(log_function);

        assert_eq!(signal.lock().unwrap().as_ref(), Some(&false));
    }

    #[test]
    fn warning_is_not_computed_when_log_level_is_error() {
        let logger = make_logger_at_level(Level::Error);
        let signal = Arc::new(Mutex::new(Some(false)));
        let signal_c = signal.clone();

        let log_function = move || {
            let mut locked_signal = signal_c.lock().unwrap();
            locked_signal.replace(true);
            "blah".to_string()
        };

        logger.warning(log_function);

        assert_eq!(signal.lock().unwrap().as_ref(), Some(&false));
    }

    #[test]
    fn trace_is_computed_when_log_level_is_trace() {
        let logger = make_logger_at_level(Level::Trace);
        let signal = Arc::new(Mutex::new(Some(false)));
        let signal_c = signal.clone();

        let log_function = move || {
            let mut locked_signal = signal_c.lock().unwrap();
            locked_signal.replace(true);
            "blah".to_string()
        };

        logger.trace(log_function);

        assert_eq!(signal.lock().unwrap().as_ref(), Some(&true));
    }

    #[test]
    fn debug_is_computed_when_log_level_is_debug() {
        let logger = make_logger_at_level(Level::Debug);
        let signal = Arc::new(Mutex::new(Some(false)));
        let signal_c = signal.clone();

        let log_function = move || {
            let mut locked_signal = signal_c.lock().unwrap();
            locked_signal.replace(true);
            "blah".to_string()
        };

        logger.debug(log_function);

        assert_eq!(signal.lock().unwrap().as_ref(), Some(&true));
    }

    #[test]
    fn info_is_computed_when_log_level_is_info() {
        let logger = make_logger_at_level(Level::Info);
        let signal = Arc::new(Mutex::new(Some(false)));
        let signal_c = signal.clone();

        let log_function = move || {
            let mut locked_signal = signal_c.lock().unwrap();
            locked_signal.replace(true);
            "blah".to_string()
        };

        logger.info(log_function);

        assert_eq!(signal.lock().unwrap().as_ref(), Some(&true));
    }

    #[test]
    fn warn_is_computed_when_log_level_is_warn() {
        let logger = make_logger_at_level(Level::Warn);
        let signal = Arc::new(Mutex::new(Some(false)));
        let signal_c = signal.clone();

        let log_function = move || {
            let mut locked_signal = signal_c.lock().unwrap();
            locked_signal.replace(true);
            "blah".to_string()
        };

        logger.warning(log_function);

        assert_eq!(signal.lock().unwrap().as_ref(), Some(&true));
    }

    #[test]
    fn error_is_computed_when_log_level_is_error() {
        let logger = make_logger_at_level(Level::Error);
        let signal = Arc::new(Mutex::new(Some(false)));
        let signal_c = signal.clone();

        let log_function = move || {
            let mut locked_signal = signal_c.lock().unwrap();
            locked_signal.replace(true);
            "blah".to_string()
        };

        logger.error(log_function);

        assert_eq!(signal.lock().unwrap().as_ref(), Some(&true));
    }

    #[test]
    fn macros_work() {
        init_test_logging();
        let logger = Logger::new("test");

        trace!(logger, "trace! {}", 42);
        debug!(logger, "debug! {}", 42);
        info!(logger, "info! {}", 42);
        warning!(logger, "warning! {}", 42);
        error!(logger, "error! {}", 42);

        let tlh = TestLogHandler::new();
        tlh.exists_log_containing("trace! 42");
        tlh.exists_log_containing("debug! 42");
        tlh.exists_log_containing("info! 42");
        tlh.exists_log_containing("warning! 42");
        tlh.exists_log_containing("error! 42");
    }

    fn timestamp_as_string(timestamp: &SystemTime) -> String {
        let date_time: DateTime<Local> = DateTime::from(timestamp.clone());
        let fmt = StrftimeItems::new("%Y-%m-%dT%H:%M:%S%.3f");
        date_time.format_with_items(fmt).to_string()
    }

    fn thread_id_as_string(thread_id: ThreadId) -> String {
        let thread_id_str = format!("{:?}", thread_id);
        String::from(&thread_id_str[9..(thread_id_str.len() - 1)])
    }

    fn assert_between(candidate: &str, before: &str, after: &str) {
        assert_eq!(
            candidate >= before,
            true,
            "{} is before the interval {} - {}",
            candidate,
            before,
            after,
        );
        assert_eq!(
            candidate <= after,
            true,
            "{} is after the interval {} - {}",
            candidate,
            before,
            after,
        );
    }

    fn make_logger_at_level(level: Level) -> Logger {
        Logger {
            name: "test".to_string(),
            #[cfg(not(feature = "no_test_share"))]
            level_limit: level,
        }
    }
}
