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
    initiation_guard: AtomicBool::new(false),
    recipient_opt: None,
};
const UI_MESSAGE_LOG_LEVEL: Level = Level::Info;

#[cfg(test)]
const TEST_LOG_RECIPIENT_GUARD: Mutex<()> = Mutex::new(());

pub struct LogRecipient {
    initiated: AtomicBool,
    initiation_guard: AtomicBool,
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
        } else {
            if unsafe {
                LOG_RECIPIENT
                    .initiation_guard
                    .swap(false, Ordering::Relaxed)
            } {
                if let Some(recipient) = unsafe { LOG_RECIPIENT.recipient_opt.as_ref() } {
                    todo!()
                }
                //unsafe LOG_RECIPIENT.guard.swap(true)
            }
        }
    }
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

// fn transmit(&self, msg: String, log_level: SerializableLogLevel) {
//     todo!("Test Me!")
//     // if let Some(recipient) = unsafe { LOG_RECIPIENT_OPT.as_ref() } {
//     //     let actor_msg = NodeToUiMessage {
//     //         target: MessageTarget::AllClients,
//     //         body: UiLogBroadcast {
//     //             msg,
//     //             log_level
//     //         }.tmb(0),
//     //     }; //TODO we probably don't want to confront all connected clients?
//     //     recipient.try_send(actor_msg).expect("UiGateway is dead")
//     // }
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
    use chrono::format::StrftimeItems;
    use chrono::{DateTime, Local};
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::{Arc, Barrier, Mutex};
    use std::thread;
    use std::thread::ThreadId;
    use std::time::{Duration, SystemTime};
    use actix::{Actor, Context, Handler, System, Message, AsyncContext};

    // 1) Send a message to Recorder
    // 2) Assert that testing actor received our message (proof that we called transmit method)
    // 3) Not going to print the logs to logfile
    struct TestUiGateway{
        received_messages: Vec<NodeToUiMessage>,
        expected_msg_count: usize,
    }

    impl TestUiGateway{
        fn new(msg_count: usize,recording_arc: Arc<Mutex<>>)->Self{
            Self{
                received_messages: ,
                expected_msg_count: msg_count
            }
        }
    }

    impl Actor for TestUiGateway{
        type Context = Context<Self>;
    }

    impl Handler<NodeToUiMessage> for TestUiGateway{
        type Result = ();

        fn handle(&mut self, msg: NodeToUiMessage, ctx: &mut Self::Context) -> Self::Result {
            self.received_messages.push(msg);
            if self.received_messages.len() == self.expected_msg_count{
                System::current().stop();
            }
        }
    }

    #[derive(Message)]
    struct ScheduleStop{
        timeout: Duration
    }

    #[derive(Message)]
    struct Stop{}

    impl Handler<ScheduleStop> for TestUiGateway{
        type Result = ();

        fn handle(&mut self, msg: ScheduleStop, ctx: &mut Self::Context) -> Self::Result {
            ctx.notify_later(Stop{},msg.timeout);
        }
    }

    impl Handler<Stop> for TestUiGateway{
        type Result = ();

        fn handle(&mut self, msg: Stop, ctx: &mut Self::Context) -> Self::Result {
            System::current().stop()
        }
    }

    #[test]
    fn transmit_log_sends_msg_from_multiple_threads_when_log_recipient_initiated() {
        fn create_msg() -> NodeToUiMessage {
            NodeToUiMessage {
                target: MessageTarget::AllClients,
                body: MessageBody {
                    opcode: "whatever".to_string(),
                    path: MessagePath::FireAndForget,
                    payload: Ok(String::from("our message")),
                },
            }
        }
        fn send_message_to_recipient() {
            let recipient = unsafe { LOG_RECIPIENT.recipient_opt.as_ref().unwrap() };
            recipient.try_send(create_msg()).unwrap()
        }
        let _test_guard = TEST_LOG_RECIPIENT_GUARD.lock().unwrap();
        let ui_gateway = TestUiGateway::default();
        let addr = ui_gateway.start();
        let recipient = addr.clone().recipient();
        unsafe {
            LOG_RECIPIENT.recipient_opt.replace(recipient);
            LOG_RECIPIENT.initiated.store(true,Ordering::Relaxed)
        }

        let system = System::new("test_system");
        addr.try_send(ScheduleStop{ timeout: Duration::from_secs(5) }).unwrap();
        addr.try_send(SendAllAtOnce{}).unwrap();
        system.run();
        let recording = recording_arc.lock().unwrap();
        assert_eq!(recording.len(), 3);
        let msg1 = recording.get_record::<NodeToUiMessage>(0);
        let msg2 = recording.get_record::<NodeToUiMessage>(1);
        let msg3 = recording.get_record::<NodeToUiMessage>(2);
        assert!(msg1 == msg2 && msg2 == &create_msg())
    }

    #[derive(Message)]
    struct SendAllAtOnce{}

    impl Handler<SendAllAtOnce> for TestUiGateway{
        type Result = ();

        fn handle(&mut self, msg: SendAllAtOnce, ctx: &mut Self::Context) -> Self::Result {
            let barrier_arc = Arc::new(Barrier::new(3));
            let mut join_handle_vector = Vec::new();
            (0..3).for_each(|_| {
                join_handle_vector.push(thread::spawn(move || {
                    let barrier_arc_clone = Arc::clone(&barrier_arc);
                    barrier_arc_clone.wait();
                    send_message_to_recipient()
                }))
            });

        }
    }

    #[test]
    fn prepare_log_recipient_waits_to_set_the_recipient() {
        unsafe { assert_eq!(LOG_RECIPIENT.initiation_guard.load(Relaxed), false) };
        // pub fn prepare_log_recipient (recipient: Recipient<NodeToUiMessage>) {
        //     // loop {
        //     //     if LOG_RECIPIENT.guard.swap(false) {
        //     //         LOG_RECIPIENT.recipient_opt = Some (recipient);
        //     //         LOG_RECIPIENT.guard.swap(true);
        //     //         break;
        //     //     }
        //     //     else {
        //     //         thread.sleep (Duration::from_millis (10)) // wait for the logger to finish
        //     //     }
        //     // }

        let before = SystemTime::now();
        let join_handle = thread::spawn(move || {});
        let _ = LogRecipient::prepare_log_recipient(recipient);
        let after = SystemTime::now();
        join_handle.join().unwrap();
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
