// Copyright (c) 2019-2021, MASQ (https://masq.ai) and/or its affiliates. All rights reserved.

use linefeed::ReadResult;
use masq_cli_lib::command_factory::CommandFactoryError::{CommandSyntax, UnrecognizedSubcommand};
use masq_cli_lib::command_factory::{CommandFactory, CommandFactoryReal};
use masq_cli_lib::command_processor::{
    CommandProcessor, CommandProcessorFactory, CommandProcessorFactoryReal,
};
use masq_cli_lib::communications::broadcast_handler::StreamFactoryReal;
use masq_cli_lib::line_reader::TerminalEvent::{
    Break, CommandLine, Continue, Error as TerminalEventError,
};
use masq_cli_lib::terminal_interface::{InterfaceReal, TerminalInterfaceFactory, TerminalWrapper};
use masq_lib::command;
use masq_lib::command::{Command, StdStreams};
use masq_lib::short_writeln;
use std::io;

fn main() {
    let mut streams: StdStreams<'_> = StdStreams {
        stdin: &mut io::stdin(),
        stdout: &mut io::stdout(),
        stderr: &mut io::stderr(),
    };

    let args: Vec<String> = std::env::args().collect();
    let streams_ref: &mut StdStreams<'_> = &mut streams;
    let exit_code = Main::new().go(streams_ref, &args);
    ::std::process::exit(i32::from(exit_code));
}

struct Main {
    command_factory: Box<dyn CommandFactory>,
    processor_factory: Box<dyn CommandProcessorFactory>,
    terminal_interface_factory: Box<dyn TerminalInterfaceFactory>,
}

impl command::Command for Main {
    fn go(&mut self, streams: &mut StdStreams<'_>, args: &[String]) -> u8 {
        let broadcast_stream_factory = StreamFactoryReal::new();
        let interface = match self.terminal_interface_factory.make() {
            Ok(interface) => interface,
            Err(error) => {
                short_writeln!(streams.stderr, "Terminal interface: {}", error);
                return 1;
            }
        };
        let mut command_processor = match self.processor_factory.make(
            Box::new(interface),
            Box::new(broadcast_stream_factory),
            args,
        ) {
            Ok(processor) => processor,
            Err(e) => {
                short_writeln!(streams.stderr, "Can't connect to Daemon or Node ({:?}). Probably this means the Daemon isn't running.", e);
                return 1;
            }
        };
        let result = match Self::extract_subcommand(args) {
            Some(command_parts) => {
                match self.handle_command(&mut *command_processor, command_parts, streams.stderr) {
                    Ok(_) => 0,
                    Err(_) => 1,
                }
            }
            None => self.go_interactive(&mut *command_processor, streams),
        };
        command_processor.close();
        result
    }
}

impl Main {
    pub fn new() -> Self {
        Self {
            command_factory: Box::new(CommandFactoryReal::new()),
            processor_factory: Box::new(CommandProcessorFactoryReal {}),
            terminal_interface_factory: Box::new(InterfaceReal {}),
        }
    }

    fn extract_subcommand(args: &[String]) -> Option<Vec<String>> {
        let args_vec: Vec<String> = args.to_vec();
        for idx in 1..args_vec.len() {
            let one = &args_vec[idx - 1];
            let two = &args_vec[idx];
            if !one.starts_with("--") && !two.starts_with("--") {
                return Some(args_vec.into_iter().skip(idx).collect());
            }
        }
        None
    }

    //TODO: make this a direct part of go interactive...too much indirection here
    fn accept_subcommand(term_interface: TerminalWrapper) -> Result<Option<Vec<String>>, String> {
        match term_interface.read_line() {
            CommandLine(line) => Ok(Some(Self::split_quoted_line(line))),
            Break => unimplemented!(),
            Continue => unimplemented!(), //Err(e),
            TerminalEventError(msg) => Err(msg),
        }
    }

    fn split_quoted_line(input: String) -> Vec<String> {
        let mut active_single = false;
        let mut active_double = false;
        let mut pieces: Vec<String> = vec![];
        let mut current_piece = String::new();
        //  input.replace("\\",r#"\"#)
        input.chars().for_each(|c| {
            if c.is_whitespace() && !active_double && !active_single {
                if !current_piece.is_empty() {
                    pieces.push(current_piece.clone());
                    current_piece.clear();
                }
            } else if c == '"' && !active_single {
                active_double = !active_double;
            } else if c == '\'' && !active_double {
                active_single = !active_single;
            } else {
                current_piece.push(c);
            }
        });
        if !current_piece.is_empty() {
            pieces.push(current_piece)
        }
        pieces
    }

    fn go_interactive(
        &self,
        processor: &mut dyn CommandProcessor,
        streams: &mut StdStreams<'_>,
    ) -> u8 {
        loop {
            let args = match Self::accept_subcommand(processor.clone_terminal_interface()) {
                Ok(Some(args)) => args,
                Ok(None) => break, //EOF
                Err(e) => {
                    short_writeln!(streams.stderr, "{}", e);
                    return 1;
                }
            };
            if args.is_empty() {
                continue;
            }
            if args[0] == "exit" {
                break;
            }
            match self.handle_command(processor, args, streams.stderr) {
                Ok(_) => (),
                Err(_) => continue,
            }
        }
        0
    }

    fn handle_command(
        &self,
        processor: &mut dyn CommandProcessor,
        command_parts: Vec<String>,
        stderr: &mut dyn io::Write,
    ) -> Result<(), ()> {
        let command = match self.command_factory.make(command_parts) {
            Ok(c) => c,
            Err(UnrecognizedSubcommand(msg)) => {
                short_writeln!(stderr, "Unrecognized command: '{}'", msg);
                return Err(());
            }
            Err(CommandSyntax(msg)) => {
                short_writeln!(stderr, "{}", msg);
                return Err(());
            }
        };
        if let Err(e) = processor.process(command) {
            short_writeln!(stderr, "{}", e);
            Err(())
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use masq_cli_lib::command_context::CommandContext;
    use masq_cli_lib::command_context::ContextError::{ConnectionRefused, Other};
    use masq_cli_lib::command_factory::CommandFactoryError;
    use masq_cli_lib::commands::commands_common;
    use masq_cli_lib::commands::commands_common::CommandError;
    use masq_cli_lib::commands::commands_common::CommandError::Transmission;
    use masq_cli_lib::line_reader::{TerminalEvent, TerminalReal};
    use masq_cli_lib::terminal_interface::{InterfaceMock, InterfaceRawMock, TerminalPassiveMock};
    use masq_cli_lib::test_utils::mocks::{
        CommandContextMock, CommandFactoryMock, CommandProcessorFactoryMock, CommandProcessorMock,
        MockCommand,
    };
    use masq_lib::messages::ToMessageBody;
    use masq_lib::messages::UiShutdownRequest;
    use masq_lib::test_utils::fake_stream_holder::{ByteArrayReader, FakeStreamHolder};
    use std::io::ErrorKind;
    use std::sync::{Arc, Mutex};
    //
    // struct BufReadFactoryMock {
    //     make_params: Arc<Mutex<Vec<Arc<Mutex<()>>>>>,
    //     interactive: RefCell<Option<ByteArrayReader>>,
    // }
    //
    // impl BufReadFactory for BufReadFactoryMock {
    //     fn make(&self, output_synchronizer: TerminalWrapper) -> Box<()> {
    //         self.make_params.lock().unwrap().push(output_synchronizer);
    //         Box::new(())
    //     }
    // }
    //
    // impl BufReadFactoryMock {
    //     pub fn new() -> BufReadFactoryMock {
    //         BufReadFactoryMock {
    //             make_params: Arc::new(Mutex::new(vec![])),
    //             interactive: RefCell::new(None),
    //         }
    //     }
    //
    //     pub fn make_params(mut self, params: Arc<Mutex<Vec<Arc<Mutex<()>>>>>) -> Self {
    //         self.make_params = params.clone();
    //         self
    //     }
    //
    //     pub fn make_interactive_result(self, input: &str) -> BufReadFactoryMock {
    //         self.make_interactive_reader(ByteArrayReader::new(input.as_bytes()))
    //     }
    //
    //     pub fn make_interactive_reader(self, reader: ByteArrayReader) -> BufReadFactoryMock {
    //         self.interactive.borrow_mut().replace(reader);
    //         self
    //     }
    // }

    #[test]
    fn noninteractive_mode_works_when_everything_is_copacetic() {
        let command = MockCommand::new(UiShutdownRequest {}.tmb(1));
        let c_make_params_arc = Arc::new(Mutex::new(vec![]));
        let command_factory = CommandFactoryMock::new()
            .make_params(&c_make_params_arc)
            .make_result(Ok(Box::new(command)));
        let interface = InterfaceMock::new()
            .make_result(Ok(TerminalReal::new(Box::new(InterfaceRawMock::new()))));
        let process_params_arc = Arc::new(Mutex::new(vec![]));
        let processor = CommandProcessorMock::new()
            .process_params(&process_params_arc)
            .process_result(Ok(()));
        let p_make_params_arc = Arc::new(Mutex::new(vec![]));
        let processor_factory = CommandProcessorFactoryMock::new()
            .make_params(&p_make_params_arc)
            .make_result(Ok(Box::new(processor)));
        let mut subject = Main {
            command_factory: Box::new(command_factory),
            processor_factory: Box::new(processor_factory),
            terminal_interface_factory: Box::new(interface),
        };

        let result = subject.go(
            &mut FakeStreamHolder::new().streams(),
            &[
                "command".to_string(),
                "--param1".to_string(),
                "value1".to_string(),
                "--param2".to_string(),
                "value2".to_string(),
                "subcommand".to_string(),
                "--param3".to_string(),
                "value3".to_string(),
                "param4".to_string(),
                "param5".to_string(),
            ],
        );

        assert_eq!(result, 0);
        let c_make_params = c_make_params_arc.lock().unwrap();
        assert_eq!(
            *c_make_params,
            vec![vec![
                "subcommand".to_string(),
                "--param3".to_string(),
                "value3".to_string(),
                "param4".to_string(),
                "param5".to_string()
            ],]
        );
        let p_make_params = p_make_params_arc.lock().unwrap();
        assert_eq!(
            *p_make_params,
            vec![vec![
                "command".to_string(),
                "--param1".to_string(),
                "value1".to_string(),
                "--param2".to_string(),
                "value2".to_string(),
                "subcommand".to_string(),
                "--param3".to_string(),
                "value3".to_string(),
                "param4".to_string(),
                "param5".to_string(),
            ]]
        );
        let mut process_params = process_params_arc.lock().unwrap();
        let command = process_params.remove(0);
        let transact_params_arc = Arc::new(Mutex::new(vec![]));
        let mut context = CommandContextMock::new()
            .transact_params(&transact_params_arc)
            .transact_result(Err(Other("not really an error".to_string())));
        let stdout_arc = context.stdout_arc();
        let stderr_arc = context.stderr_arc();

        let result = command.execute(&mut context);

        assert_eq!(
            result,
            Err(Transmission("Other(\"not really an error\")".to_string()))
        );
        let transact_params = transact_params_arc.lock().unwrap();
        assert_eq!(*transact_params, vec![(UiShutdownRequest {}.tmb(1), 1000)]);
        assert_eq!(
            stdout_arc.lock().unwrap().get_string(),
            "MockCommand output".to_string()
        );
        assert_eq!(
            stderr_arc.lock().unwrap().get_string(),
            "MockCommand error".to_string()
        );
    }

    #[derive(Debug)]
    struct FakeCommand {
        output: String,
    }

    impl commands_common::Command for FakeCommand {
        fn execute(&self, _context: &mut dyn CommandContext) -> Result<(), CommandError> {
            unimplemented!()
        }
    }

    impl FakeCommand {
        pub fn new(output: &str) -> Self {
            Self {
                output: output.to_string(),
            }
        }
    }

    #[test]
    fn interactive_mode_works_when_everything_is_copacetic() {
        let make_params_arc = Arc::new(Mutex::new(vec![]));
        let command_factory = CommandFactoryMock::new()
            .make_params(&make_params_arc)
            .make_result(Ok(Box::new(FakeCommand::new("setup command"))))
            .make_result(Ok(Box::new(FakeCommand::new("start command"))));
        let terminal_mock = TerminalPassiveMock::new()
            .read_line_result(TerminalEvent::CommandLine("setup".to_string()))
            .read_line_result(TerminalEvent::CommandLine("start".to_string()))
            .read_line_result(TerminalEvent::CommandLine("exit".to_string()));
        let interface = InterfaceMock::new()
            .make_result(Ok(TerminalReal::new(Box::new(InterfaceRawMock::new()))));
        let processor = CommandProcessorMock::new()
            .process_result(Ok(()))
            .process_result(Ok(()))
            .insert_terminal_interface(TerminalWrapper::new(Box::new(terminal_mock)));
        let processor_factory =
            CommandProcessorFactoryMock::new().make_result(Ok(Box::new(processor)));
        let mut subject = Main {
            command_factory: Box::new(command_factory),
            processor_factory: Box::new(processor_factory),
            terminal_interface_factory: Box::new(interface),
        };
        let mut stream_holder = FakeStreamHolder::new();

        let result = subject.go(
            &mut stream_holder.streams(),
            &[
                "command".to_string(),
                "--param1".to_string(),
                "value1".to_string(),
            ],
        );

        assert_eq!(result, 0);
        let make_params = make_params_arc.lock().unwrap();
        assert_eq!(
            *make_params,
            vec![vec!["setup".to_string()], vec!["start".to_string()]]
        );
    }

    #[test]
    fn interactive_mode_works_for_stdin_read_error() {
        let command_factory = CommandFactoryMock::new();
        let close_params_arc = Arc::new(Mutex::new(vec![]));
        let processor = CommandProcessorMock::new()
            .close_params(&close_params_arc)
            .insert_terminal_interface(TerminalWrapper::new(Box::new(
                TerminalPassiveMock::new()
                    .read_line_result(TerminalEvent::Error("ConnectionRefused".to_string())),
            )));
        let processor_factory =
            CommandProcessorFactoryMock::new().make_result(Ok(Box::new(processor)));
        // let buf_read_factory = BufReadFactoryMock::new().make_interactive_reader(
        //     ByteArrayReader::new(&[0; 0])
        //         .reject_next_read(std::io::Error::from(ErrorKind::ConnectionRefused)),
        // );
        let interface = InterfaceMock::new()
            .make_result(Ok(TerminalReal::new(Box::new(InterfaceRawMock::new()))));
        let mut subject = Main {
            command_factory: Box::new(command_factory),
            processor_factory: Box::new(processor_factory),
            terminal_interface_factory: Box::new(interface),
        };
        let mut stream_holder = FakeStreamHolder::new();

        let result = subject.go(&mut stream_holder.streams(), &["command".to_string()]);

        assert_eq!(result, 1);
        assert_eq!(
            stream_holder.stderr.get_string(),
            "ConnectionRefused\n".to_string()
        );
        let close_params = close_params_arc.lock().unwrap();
        assert_eq!(close_params.len(), 1);
    }

    #[test]
    fn interactive_mode_works_for_unrecognized_command() {
        let make_params_arc = Arc::new(Mutex::new(vec![]));
        let command_factory = CommandFactoryMock::new()
            .make_params(&make_params_arc)
            .make_result(Err(CommandFactoryError::UnrecognizedSubcommand(
                "Booga!".to_string(),
            )));
        let interface = InterfaceMock::new()
            .make_result(Ok(TerminalReal::new(Box::new(InterfaceRawMock::new()))));
        let processor =
            CommandProcessorMock::new().insert_terminal_interface(TerminalWrapper::new(Box::new(
                TerminalPassiveMock::new()
                    .read_line_result(TerminalEvent::CommandLine("error command\n".to_string()))
                    .read_line_result(TerminalEvent::CommandLine("exit\n".to_string())),
            )));
        let processor_factory =
            CommandProcessorFactoryMock::new().make_result(Ok(Box::new(processor)));
        let mut subject = Main {
            command_factory: Box::new(command_factory),
            processor_factory: Box::new(processor_factory),
            // buf_read_factory: Box::new(
            //     BufReadFactoryMock::new().make_interactive_result("error command\nexit\n"),
            // ),
            terminal_interface_factory: Box::new(interface),
        };
        let mut stream_holder = FakeStreamHolder::new();

        let result = subject.go(&mut stream_holder.streams(), &["command".to_string()]);

        assert_eq!(result, 0);
        let make_params = make_params_arc.lock().unwrap();
        assert_eq!(
            *make_params,
            vec![vec!["error".to_string(), "command".to_string()]]
        );
        assert_eq!(
            stream_holder.stderr.get_string(),
            "Unrecognized command: 'Booga!'\n".to_string()
        );
    }

    #[test]
    fn interactive_mode_works_for_command_with_bad_syntax() {
        let make_params_arc = Arc::new(Mutex::new(vec![]));
        let command_factory = CommandFactoryMock::new()
            .make_params(&make_params_arc)
            .make_result(Err(CommandFactoryError::CommandSyntax(
                "Booga!".to_string(),
            )));
        let interface = InterfaceMock::new()
            .make_result(Ok(TerminalReal::new(Box::new(InterfaceRawMock::new()))));
        let processor =
            CommandProcessorMock::new().insert_terminal_interface(TerminalWrapper::new(Box::new(
                TerminalPassiveMock::new()
                    .read_line_result(TerminalEvent::CommandLine("error command\n".to_string()))
                    .read_line_result(TerminalEvent::CommandLine("exit\n".to_string())),
            )));
        let processor_factory =
            CommandProcessorFactoryMock::new().make_result(Ok(Box::new(processor)));
        let mut subject = Main {
            command_factory: Box::new(command_factory),
            processor_factory: Box::new(processor_factory),
            // buf_read_factory: Box::new(
            //     BufReadFactoryMock::new().make_interactive_result("error command\nexit\n"),
            // ),
            terminal_interface_factory: Box::new(interface),
        };
        let mut stream_holder = FakeStreamHolder::new();

        let result = subject.go(&mut stream_holder.streams(), &["command".to_string()]);

        assert_eq!(result, 0);
        let make_params = make_params_arc.lock().unwrap();
        assert_eq!(
            *make_params,
            vec![vec!["error".to_string(), "command".to_string()]]
        );
        assert_eq!(stream_holder.stderr.get_string(), "Booga!\n".to_string());
    }

    #[test]
    fn clone_of_synchronizer_is_shared_along_and_passed_on_properly() {
        let make_params_arc = Arc::new(Mutex::new(vec![]));
        let command_factory = CommandFactoryMock::new()
            .make_params(&make_params_arc)
            .make_result(Ok(Box::new(FakeCommand::new("setup command"))));
        let interface = InterfaceMock::new()
            .make_result(Ok(TerminalReal::new(Box::new(InterfaceRawMock::new()))));
        let terminal_interface_reference_for_inner = TerminalWrapper::new(Box::new(
            TerminalPassiveMock::new()
                .read_line_result(TerminalEvent::CommandLine("setup\n".to_string()))
                .read_line_result(TerminalEvent::CommandLine("exit\n".to_string())),
        ));
        let reference_for_counting = Arc::new(Mutex::new(0));
        let processor = CommandProcessorMock::new()
            .insert_terminal_interface(terminal_interface_reference_for_inner.clone())
            .insert_terminal_wrapper_shared_counter(reference_for_counting.clone())
            .process_result(Ok(()));

        assert_eq!(*reference_for_counting.lock().unwrap(), 0);

        let processor_factory =
            CommandProcessorFactoryMock::new().make_result(Ok(Box::new(processor)));
        let mut subject = Main {
            command_factory: Box::new(command_factory),
            processor_factory: Box::new(processor_factory),
            // buf_read_factory: Box::new(
            //     BufReadFactoryMock::new()
            //         .make_interactive_result("setup\n\nexit\n")
            //         .make_params(buf_read_sync_params_arc.clone()),
            // ),
            terminal_interface_factory: Box::new(interface),
        };
        let mut stream_holder = FakeStreamHolder::new();

        let result = subject.go(
            &mut stream_holder.streams(),
            &[
                "command".to_string(),
                "--param1".to_string(),
                "value1".to_string(),
            ],
        );

        //cloned once for each command, so twice in total
        assert_eq!(*reference_for_counting.lock().unwrap(), 2);

        assert_eq!(result, 0);
        let make_params = make_params_arc.lock().unwrap();
        assert_eq!(*make_params, vec![vec!["setup".to_string()]]);
    }

    #[test]
    fn accept_subcommand_handles_balanced_double_quotes() {
        let interface_mock = TerminalWrapper::new(Box::new(
            TerminalPassiveMock::new().read_line_result(TerminalEvent::CommandLine(
                r#"  first \"second\" third  \"fourth'fifth\" \t sixth \"seventh eighth\tninth\" "#
                    .to_string(),
            )),
        ));
        let result = Main::accept_subcommand(interface_mock).unwrap();

        assert_eq!(
            result,
            Some(vec![
                "first".to_string(),
                "second".to_string(),
                "third".to_string(),
                "fourth'fifth".to_string(),
                "sixth".to_string(),
                "seventh eighth\tninth".to_string(),
            ])
        )
    }

    #[test]
    fn accept_subcommand_handles_unbalanced_double_quotes() {
        let interface_mock = TerminalWrapper::new(Box::new(
            TerminalPassiveMock::new().read_line_result(TerminalEvent::CommandLine(
                r#"  first \"second\" third  \"fourth'fifth\" \t sixth \"seventh eighth\tninth  "#
                    .to_string(),
            )),
        ));

        let result = Main::accept_subcommand(interface_mock).unwrap();

        assert_eq!(
            result,
            Some(vec![
                "first".to_string(),
                "second".to_string(),
                "third".to_string(),
                "fourth'fifth".to_string(),
                "sixth".to_string(),
                "seventh eighth\tninth  ".to_string(),
            ])
        )
    }

    #[test]
    fn accept_subcommand_handles_balanced_single_quotes() {
        let interface_mock =
            TerminalWrapper::new(Box::new(TerminalPassiveMock::new().read_line_result(
                TerminalEvent::CommandLine(r#"  first \n 'second' \n third \n 'fourth\"fifth' \t sixth 'seventh eighth\tninth' "#.to_string()
                ))));

        let result = Main::accept_subcommand(interface_mock).unwrap();

        assert_eq!(
            result,
            Some(vec![
                "first".to_string(),
                "second".to_string(),
                "third".to_string(),
                "fourth\"fifth".to_string(),
                "sixth".to_string(),
                "seventh eighth\tninth".to_string(),
            ])
        )
    }

    #[test]
    fn accept_subcommand_handles_unbalanced_single_quotes() {
        let interface_mock = TerminalWrapper::new(Box::new(
            TerminalPassiveMock::new().read_line_result(TerminalEvent::CommandLine(
                r#"  first 'second' third  'fourth\"fifth' \t sixth 'seventh eighth\tninth  "#
                    .to_string(),
            )),
        ));
        let result = Main::accept_subcommand(interface_mock).unwrap();

        assert_eq!(
            result,
            Some(vec![
                "first".to_string(),
                "second".to_string(),
                "third".to_string(),
                "fourth\"fifth".to_string(),
                "sixth".to_string(),
                "seventh eighth\tninth  ".to_string(),
            ])
        )
    }

    #[test]
    fn go_works_when_error_turns_up_in_interface_factory() {
        let c_make_params_arc = Arc::new(Mutex::new(vec![]));
        let command_factory = CommandFactoryMock::new().make_params(&c_make_params_arc);
        let interface = InterfaceMock::new().make_result(Err("Invalid handle".to_string()));
        let mut subject = Main {
            command_factory: Box::new(command_factory),
            processor_factory: Box::new(CommandProcessorFactoryMock::new()),
            terminal_interface_factory: Box::new(interface),
        };
        let mut stream_holder = FakeStreamHolder::new();

        let result = subject.go(
            &mut stream_holder.streams(),
            &["command".to_string(), "subcommand".to_string()],
        );

        assert_eq!(result, 1);
        let c_make_params = c_make_params_arc.lock().unwrap();
        assert!(c_make_params.is_empty());
        assert_eq!(
            stream_holder.stderr.get_string(),
            "Terminal interface: Invalid handle\n".to_string()
        );
    }

    #[test]
    fn go_works_when_command_is_unrecognized() {
        let c_make_params_arc = Arc::new(Mutex::new(vec![]));
        let command_factory = CommandFactoryMock::new()
            .make_params(&c_make_params_arc)
            .make_result(Err(UnrecognizedSubcommand("booga".to_string())));
        let close_params_arc = Arc::new(Mutex::new(vec![]));
        let interface = InterfaceMock::new()
            .make_result(Ok(TerminalReal::new(Box::new(InterfaceRawMock::new()))));
        let processor = CommandProcessorMock::new().close_params(&close_params_arc);
        let processor_factory =
            CommandProcessorFactoryMock::new().make_result(Ok(Box::new(processor)));
        let mut subject = Main {
            command_factory: Box::new(command_factory),
            processor_factory: Box::new(processor_factory),
            terminal_interface_factory: Box::new(interface),
        };
        let mut stream_holder = FakeStreamHolder::new();

        let result = subject.go(
            &mut stream_holder.streams(),
            &["command".to_string(), "subcommand".to_string()],
        );

        assert_eq!(result, 1);
        let c_make_params = c_make_params_arc.lock().unwrap();
        assert_eq!(*c_make_params, vec![vec!["subcommand".to_string()],]);
        assert_eq!(
            stream_holder.stderr.get_string(),
            "Unrecognized command: 'booga'\n".to_string()
        );
        let close_params = close_params_arc.lock().unwrap();
        assert_eq!(close_params.len(), 1);
    }

    #[test]
    fn go_works_when_command_has_bad_syntax() {
        let c_make_params_arc = Arc::new(Mutex::new(vec![]));
        let command_factory = CommandFactoryMock::new()
            .make_params(&c_make_params_arc)
            .make_result(Err(CommandSyntax("booga".to_string())));
        let interface = InterfaceMock::new()
            .make_result(Ok(TerminalReal::new(Box::new(InterfaceRawMock::new()))));
        let processor = CommandProcessorMock::new();
        let processor_factory =
            CommandProcessorFactoryMock::new().make_result(Ok(Box::new(processor)));
        let mut subject = Main {
            command_factory: Box::new(command_factory),
            processor_factory: Box::new(processor_factory),
            terminal_interface_factory: Box::new(interface),
        };
        let mut stream_holder = FakeStreamHolder::new();

        let result = subject.go(
            &mut stream_holder.streams(),
            &["command".to_string(), "subcommand".to_string()],
        );

        assert_eq!(result, 1);
        let c_make_params = c_make_params_arc.lock().unwrap();
        assert_eq!(*c_make_params, vec![vec!["subcommand".to_string()],]);
        assert_eq!(stream_holder.stdout.get_string(), "".to_string());
        assert_eq!(stream_holder.stderr.get_string(), "booga\n".to_string());
    }

    #[test]
    fn go_works_when_command_execution_fails() {
        let command = MockCommand::new(UiShutdownRequest {}.tmb(1)).execute_result(Ok(())); // irrelevant
        let command_factory = CommandFactoryMock::new().make_result(Ok(Box::new(command)));
        let process_params_arc = Arc::new(Mutex::new(vec![]));
        let interface = InterfaceMock::new()
            .make_result(Ok(TerminalReal::new(Box::new(InterfaceRawMock::new()))));
        let processor = CommandProcessorMock::new()
            .process_params(&process_params_arc)
            .process_result(Err(Transmission("Booga!".to_string())));
        let processor_factory =
            CommandProcessorFactoryMock::new().make_result(Ok(Box::new(processor)));
        let mut subject = Main {
            command_factory: Box::new(command_factory),
            processor_factory: Box::new(processor_factory),
            terminal_interface_factory: Box::new(interface),
        };
        let mut stream_holder = FakeStreamHolder::new();

        let result = subject.go(
            &mut stream_holder.streams(),
            &["command".to_string(), "subcommand".to_string()],
        );

        assert_eq!(result, 1);
        assert_eq!(stream_holder.stdout.get_string(), "".to_string());
        assert_eq!(
            stream_holder.stderr.get_string(),
            "Transmission problem: Booga!\n".to_string()
        );
    }

    #[test]
    fn go_works_when_daemon_is_not_running() {
        let interface = InterfaceMock::new()
            .make_result(Ok(TerminalReal::new(Box::new(InterfaceRawMock::new()))));
        let processor_factory = CommandProcessorFactoryMock::new()
            .make_result(Err(CommandError::ConnectionProblem("booga".to_string())));
        let mut subject = Main {
            command_factory: Box::new(CommandFactoryMock::new()),
            processor_factory: Box::new(processor_factory),
            terminal_interface_factory: Box::new(interface),
        };
        let mut stream_holder = FakeStreamHolder::new();

        let result = subject.go(
            &mut stream_holder.streams(),
            &["command".to_string(), "subcommand".to_string()],
        );

        assert_eq!(result, 1);
        assert_eq!(stream_holder.stdout.get_string(), "".to_string());
        assert_eq!(
            stream_holder.stderr.get_string(),
            "Can't connect to Daemon or Node (ConnectionProblem(\"booga\")). Probably this means the Daemon isn't running.\n".to_string()
        );
    }
}
