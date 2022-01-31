// Copyright (c) 2019, MASQ (https://masq.ai) and/or its affiliates. All rights reserved.

pub mod configurator;
pub mod node_configurator_initialization;
pub mod node_configurator_standard;

use crate::bootstrapper::RealUser;
use crate::database::db_initializer::{DbInitializer, DbInitializerReal, DATABASE_FILE};
use crate::database::db_migrations::MigratorConfig;
use crate::db_config::persistent_configuration::{
    PersistentConfiguration, PersistentConfigurationReal,
};
use crate::sub_lib::utils::make_new_multi_config;
use clap::{value_t, App};
use dirs::{data_local_dir, home_dir};
use masq_lib::blockchains::chains::Chain;
use masq_lib::constants::DEFAULT_CHAIN;
use masq_lib::multi_config::{merge, CommandLineVcl, EnvironmentVcl, MultiConfig, VclArg};
use masq_lib::shared_schema::{
    chain_arg, config_file_arg, data_directory_arg, real_user_arg, ConfiguratorError,
};
use masq_lib::utils::{localhost, ExpectValue, WrapResult};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread::JoinHandle;
use std::time::Duration;
use std::{io, net};

pub trait NodeConfigurator<T> {
    fn configure(&self, multi_config: &MultiConfig) -> Result<T, ConfiguratorError>;
}

pub fn determine_config_file_path(
    dirs_wrapper: &dyn DirsWrapper,
    app: &App,
    args: &[String],
) -> Result<(PathBuf, bool), ConfiguratorError> {
    let orientation_schema = App::new("MASQNode")
        .arg(chain_arg())
        .arg(real_user_arg())
        .arg(data_directory_arg())
        .arg(config_file_arg());
    let orientation_args: Vec<Box<dyn VclArg>> = merge(
        Box::new(EnvironmentVcl::new(app)),
        Box::new(CommandLineVcl::new(args.to_vec())),
    )
    .vcl_args()
    .into_iter()
    .filter(|vcl_arg| {
        (vcl_arg.name() == "--chain")
            || (vcl_arg.name() == "--real-user")
            || (vcl_arg.name() == "--data-directory")
            || (vcl_arg.name() == "--config-file")
    })
    .map(|vcl_arg| vcl_arg.dup())
    .collect();
    let orientation_vcl = CommandLineVcl::from(orientation_args);
    let multi_config = make_new_multi_config(&orientation_schema, vec![Box::new(orientation_vcl)])?;
    let config_file_path = value_m!(multi_config, "config-file", PathBuf).expectv("config-file");
    let user_specified = multi_config.occurrences_of("config-file") > 0;
    let (real_user, data_directory_opt, chain) =
        real_user_data_directory_opt_and_chain(dirs_wrapper, &multi_config);
    let directory =
        data_directory_from_context(dirs_wrapper, &real_user, &data_directory_opt, chain);
    (directory.join(config_file_path), user_specified).wrap_to_ok()
}

pub fn initialize_database(
    data_directory: &Path,
    create_if_necessary: bool,
    migrator_config: MigratorConfig,
) -> Box<dyn PersistentConfiguration> {
    let conn = DbInitializerReal::default()
        .initialize(data_directory, create_if_necessary, migrator_config)
        .unwrap_or_else(|e| {
            panic!(
                "Can't initialize database at {:?}: {:?}",
                data_directory.join(DATABASE_FILE),
                e
            )
        });
    Box::new(PersistentConfigurationReal::from(conn))
}

pub fn real_user_from_multi_config_or_populate(
    multi_config: &MultiConfig,
    dirs_wrapper: &dyn DirsWrapper,
) -> RealUser {
    match value_m!(multi_config, "real-user", RealUser) {
        None => RealUser::new(None, None, None).populate(dirs_wrapper),
        Some(real_user) => real_user.populate(dirs_wrapper),
    }
}

pub fn real_user_data_directory_opt_and_chain(
    dirs_wrapper: &dyn DirsWrapper,
    multi_config: &MultiConfig,
) -> (RealUser, Option<PathBuf>, Chain) {
    let real_user = real_user_from_multi_config_or_populate(multi_config, dirs_wrapper);
    let chain_name = value_m!(multi_config, "chain", String)
        .unwrap_or_else(|| DEFAULT_CHAIN.rec().literal_identifier.to_string());
    let data_directory_opt = value_m!(multi_config, "data-directory", PathBuf);
    (
        real_user,
        data_directory_opt,
        Chain::from(chain_name.as_str()),
    )
}

pub fn data_directory_from_context(
    dirs_wrapper: &dyn DirsWrapper,
    real_user: &RealUser,
    data_directory_opt: &Option<PathBuf>,
    chain: Chain,
) -> PathBuf {
    match data_directory_opt {
        Some(data_directory) => data_directory.clone(),
        None => {
            let right_home_dir = real_user
                .home_dir_opt
                .as_ref()
                .expect("No real-user home directory; specify --real-user")
                .to_string_lossy()
                .to_string();
            let wrong_home_dir = dirs_wrapper
                .home_dir()
                .expect("No privileged home directory; specify --data-directory")
                .to_string_lossy()
                .to_string();
            let wrong_local_data_dir = dirs_wrapper
                .data_dir()
                .expect("No privileged local data directory; specify --data-directory")
                .to_string_lossy()
                .to_string();
            let right_local_data_dir =
                wrong_local_data_dir.replace(&wrong_home_dir, &right_home_dir);
            PathBuf::from(right_local_data_dir)
                .join("MASQ")
                .join(chain.rec().literal_identifier)
        }
    }
}

pub fn port_is_busy(port: u16) -> bool {
    TcpListener::bind(SocketAddr::new(localhost(), port)).is_err()
}

#[allow(dead_code)]
// this will be common for all our sockets
fn new_socket(addr: &SocketAddr) -> io::Result<Socket> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };

    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    // we're going to use read timeouts so that we don't hang waiting for packets
    socket.set_read_timeout(Some(Duration::from_millis(100)))?;
    Ok(socket)
}

#[allow(dead_code)]
/// On Windows, unlike all Unix variants, it is improper to bind to the multicast address
///
/// see https://msdn.microsoft.com/en-us/library/windows/desktop/ms737550(v=vs.85).aspx
#[cfg(windows)]
fn bind_multicast(socket: &Socket, addr: &SocketAddr) -> io::Result<()> {
    let addr = match *addr {
        SocketAddr::V4(addr) => SocketAddr::new(Ipv4Addr::new(0, 0, 0, 0).into(), addr.port()),
        SocketAddr::V6(addr) => {
            SocketAddr::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0).into(), addr.port())
        }
    };
    socket.set_reuse_address(true)?;
    socket.bind(&socket2::SockAddr::from(addr))
}

#[allow(dead_code)]
/// On unixes we bind to the multicast address, which causes multicast packets to be filtered
#[cfg(unix)]
fn bind_multicast(socket: &Socket, addr: &SocketAddr) -> io::Result<()> {
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    socket.bind(&socket2::SockAddr::from(*addr))
}

#[allow(dead_code)]
fn join_multicast(addr: SocketAddr) -> io::Result<UdpSocket> {
    let ip_addr = addr.ip();

    let socket = new_socket(&addr)?;

    // depending on the IP protocol we have slightly different work
    match ip_addr {
        IpAddr::V4(ref mdns_v4) => {
            // join to the multicast address, with all interfaces
            socket.join_multicast_v4(mdns_v4, &Ipv4Addr::new(0, 0, 0, 0))?;
        }
        IpAddr::V6(ref mdns_v6) => {
            // join to the multicast address, with all interfaces (ipv6 uses indexes not addresses)
            socket.join_multicast_v6(mdns_v6, 0)?;
            socket.set_only_v6(true)?;
        }
    };

    // bind us to the socket address.
    bind_multicast(&socket, &addr)?;

    // convert to standard sockets
    Ok(net::UdpSocket::from(socket))
}

#[allow(dead_code)]
fn multicast_listener(
    response: &'static str,
    client_done: Arc<AtomicBool>,
    addr: SocketAddr,
) -> JoinHandle<()> {
    // A barrier to not start the client test code until after the server is running
    let server_barrier = Arc::new(Barrier::new(2));
    let client_barrier = Arc::clone(&server_barrier);

    let join_handle = std::thread::Builder::new()
        .name(format!("{}:server", response))
        .spawn(move || {
            // socket creation will go here...
            let listener = join_multicast(addr).expect("failed to create listener");
            println!("{}:server: joined: {}", response, addr);

            server_barrier.wait();
            println!("{}:server: is ready", response);

            // We'll be looping until the client indicates it is done.
            while !client_done.load(std::sync::atomic::Ordering::Relaxed) {
                // test receive and response code will go here...
                let mut buf = [0u8; 64]; // receive buffer

                // we're assuming failures were timeouts, the client_done loop will stop us
                match listener.recv_from(&mut buf) {
                    Ok((len, remote_addr)) => {
                        let data = &buf[..len];

                        println!(
                            "{}:server: got data: {} from: {}",
                            response,
                            String::from_utf8_lossy(data),
                            remote_addr
                        );

                        // create a socket to send the response
                        let responder: UdpSocket = new_socket(&remote_addr)
                            .expect("failed to create responder")
                            .into();

                        // we send the response that was set at the method beginning
                        responder
                            .send_to(response.as_bytes(), &remote_addr)
                            .expect("failed to respond");

                        println!("{}:server: sent response to: {}", response, remote_addr);
                    }
                    Err(err) => {
                        println!("{}:server: got an error: {}", response, err);
                    }
                }
            }

            println!("{}:server: client is done", response);
        })
        .unwrap();

    client_barrier.wait();
    join_handle
}

#[allow(dead_code)]
fn new_sender(addr: &SocketAddr) -> io::Result<UdpSocket> {
    let socket = new_socket(addr)?;
    socket.set_multicast_if_v4(&Ipv4Addr::new(0, 0, 0, 0))?;
    socket.set_reuse_address(true)?;
    socket.bind(&SockAddr::from(SocketAddr::new(
        Ipv4Addr::new(0, 0, 0, 0).into(),
        0,
    )))?;

    // convert to standard sockets...
    Ok(net::UdpSocket::from(socket))
}

#[allow(dead_code)]
/// This will guarantee we always tell the server to stop
struct NotifyServer(Arc<AtomicBool>);
impl Drop for NotifyServer {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

pub trait DirsWrapper: Send {
    fn data_dir(&self) -> Option<PathBuf>;
    fn home_dir(&self) -> Option<PathBuf>;
    fn dup(&self) -> Box<dyn DirsWrapper>; // because implementing Clone for traits is problematic.
}

pub struct DirsWrapperReal;

impl DirsWrapper for DirsWrapperReal {
    fn data_dir(&self) -> Option<PathBuf> {
        data_local_dir()
    }
    fn home_dir(&self) -> Option<PathBuf> {
        home_dir()
    }
    fn dup(&self) -> Box<dyn DirsWrapper> {
        Box::new(DirsWrapperReal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_test_utils::DirsWrapperMock;
    use crate::test_utils::ArgsBuilder;
    use masq_lib::test_utils::environment_guard::EnvironmentGuard;
    use masq_lib::utils::find_free_port;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, UdpSocket};

    fn determine_config_file_path_app() -> App<'static, 'static> {
        App::new("test")
            .arg(data_directory_arg())
            .arg(config_file_arg())
    }

    #[test]
    fn data_directory_from_context_creates_new_folder_for_every_blockchain_platform() {
        let dirs_wrapper = DirsWrapperMock::new()
            .home_dir_result(Some(PathBuf::from("/nonexistent_home/root".to_string())))
            .data_dir_result(Some(PathBuf::from("/nonexistent_home/root/.local/share")));
        let real_user = RealUser::new(
            None,
            None,
            Some(PathBuf::from(
                "/nonexistent_home/nonexistent_alice".to_string(),
            )),
        );
        let data_dir_opt = None;
        let chain_name = "eth-ropsten";

        let result = data_directory_from_context(
            &dirs_wrapper,
            &real_user,
            &data_dir_opt,
            Chain::from(chain_name),
        );

        assert_eq!(
            result,
            PathBuf::from(
                "/nonexistent_home/nonexistent_alice/.local/share/MASQ/eth-ropsten".to_string()
            )
        )
    }

    #[test]
    fn determine_config_file_path_finds_path_in_args() {
        let _guard = EnvironmentGuard::new();
        let args = ArgsBuilder::new()
            .param("--clandestine-port", "2345")
            .param("--data-directory", "data-dir")
            .param("--config-file", "booga.toml");
        let args_vec: Vec<String> = args.into();

        let (config_file_path, user_specified) = determine_config_file_path(
            &DirsWrapperReal {},
            &determine_config_file_path_app(),
            args_vec.as_slice(),
        )
        .unwrap();

        assert_eq!(
            &format!("{}", config_file_path.parent().unwrap().display()),
            "data-dir",
        );
        assert_eq!("booga.toml", config_file_path.file_name().unwrap());
        assert_eq!(true, user_specified);
    }

    #[test]
    fn determine_config_file_path_finds_path_in_environment() {
        let _guard = EnvironmentGuard::new();
        let args = ArgsBuilder::new();
        let args_vec: Vec<String> = args.into();
        std::env::set_var("MASQ_DATA_DIRECTORY", "data_dir");
        std::env::set_var("MASQ_CONFIG_FILE", "booga.toml");

        let (config_file_path, user_specified) = determine_config_file_path(
            &DirsWrapperReal {},
            &determine_config_file_path_app(),
            args_vec.as_slice(),
        )
        .unwrap();

        assert_eq!(
            "data_dir",
            &format!("{}", config_file_path.parent().unwrap().display())
        );
        assert_eq!("booga.toml", config_file_path.file_name().unwrap());
        assert_eq!(true, user_specified);
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn determine_config_file_path_ignores_data_dir_if_config_file_has_root() {
        let _guard = EnvironmentGuard::new();
        let args = ArgsBuilder::new()
            .param("--data-directory", "data-dir")
            .param("--config-file", "/tmp/booga.toml");
        let args_vec: Vec<String> = args.into();

        let (config_file_path, user_specified) = determine_config_file_path(
            &DirsWrapperReal {},
            &determine_config_file_path_app(),
            args_vec.as_slice(),
        )
        .unwrap();

        assert_eq!(
            "/tmp/booga.toml",
            &format!("{}", config_file_path.display())
        );
        assert_eq!(true, user_specified);
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn determine_config_file_path_ignores_data_dir_if_config_file_has_separator_root() {
        let _guard = EnvironmentGuard::new();
        let args = ArgsBuilder::new()
            .param("--data-directory", "data-dir")
            .param("--config-file", r"\tmp\booga.toml");
        let args_vec: Vec<String> = args.into();

        let (config_file_path, user_specified) = determine_config_file_path(
            &DirsWrapperReal {},
            &determine_config_file_path_app(),
            args_vec.as_slice(),
        )
        .unwrap();

        assert_eq!(
            r"\tmp\booga.toml",
            &format!("{}", config_file_path.display())
        );
        assert_eq!(true, user_specified);
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn determine_config_file_path_ignores_data_dir_if_config_file_has_drive_root() {
        let _guard = EnvironmentGuard::new();
        let args = ArgsBuilder::new()
            .param("--data-directory", "data-dir")
            .param("--config-file", r"c:\tmp\booga.toml");
        let args_vec: Vec<String> = args.into();

        let (config_file_path, user_specified) = determine_config_file_path(
            &DirsWrapperReal {},
            &determine_config_file_path_app(),
            args_vec.as_slice(),
        )
        .unwrap();

        assert_eq!(
            r"c:\tmp\booga.toml",
            &format!("{}", config_file_path.display())
        );
        assert_eq!(true, user_specified);
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn determine_config_file_path_ignores_data_dir_if_config_file_has_network_root() {
        let _guard = EnvironmentGuard::new();
        let args = ArgsBuilder::new()
            .param("--data-directory", "data-dir")
            .param("--config-file", r"\\TMP\booga.toml");
        let args_vec: Vec<String> = args.into();

        let (config_file_path, user_specified) = determine_config_file_path(
            &DirsWrapperReal {},
            &determine_config_file_path_app(),
            args_vec.as_slice(),
        )
        .unwrap();

        assert_eq!(
            r"\\TMP\booga.toml",
            &format!("{}", config_file_path.display())
        );
        assert_eq!(true, user_specified);
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn determine_config_file_path_ignores_data_dir_if_config_file_has_drive_letter_but_no_separator(
    ) {
        let _guard = EnvironmentGuard::new();
        let args = ArgsBuilder::new()
            .param("--data-directory", "data-dir")
            .param("--config-file", r"c:tmp\booga.toml");
        let args_vec: Vec<String> = args.into();

        let (config_file_path, user_specified) = determine_config_file_path(
            &DirsWrapperReal {},
            &determine_config_file_path_app(),
            args_vec.as_slice(),
        )
        .unwrap();

        assert_eq!(
            r"c:tmp\booga.toml",
            &format!("{}", config_file_path.display())
        );
        assert_eq!(true, user_specified);
    }

    #[test]
    pub fn port_is_busy_detects_free_port() {
        let port = find_free_port();

        let result = port_is_busy(port);

        assert_eq!(result, false);
    }

    #[test]
    pub fn port_is_busy_detects_busy_port() {
        let port = find_free_port();
        let _listener = TcpListener::bind(SocketAddr::new(localhost(), port)).unwrap();

        let result = port_is_busy(port);

        assert_eq!(result, true);
    }

    #[test]
    fn udp_loopback_receiver_works_ipv4_stdnet() {
        let port = find_free_port();
        let ip = Ipv4Addr::new(127, 0, 0, 1);
        let socket_addr = SocketAddr::new(IpAddr::from(ip), port);
        let socket = UdpSocket::bind(socket_addr).expect("couldn't bind to address");

        handle_socket_error(&socket);
        assert_eq!(socket.local_addr().unwrap(), socket_addr);
        socket
            .connect(socket_addr)
            .expect("connect function failed");

        socket.send(&[0; 10]).expect("couldn't send message");

        handle_socket_receive(&socket);
    }

    #[test]
    #[should_panic]
    fn udp_loopback_receiver_handles_buffer_overflow() {
        let port = find_free_port();
        let ip = Ipv4Addr::new(127, 0, 0, 1);
        let socket_addr = SocketAddr::new(IpAddr::from(ip), port);
        let socket = UdpSocket::bind(socket_addr).expect("couldn't bind to address");

        handle_socket_error(&socket);
        assert_eq!(socket.local_addr().unwrap(), socket_addr);

        socket
            .connect(socket_addr)
            .expect("connect function failed");
        socket.send(&[0; 50]).expect("couldn't send data");

        handle_socket_receive(&socket);
    }

    fn handle_socket_receive(socket: &UdpSocket) {
        let mut buf = [0; 10];
        match socket.recv(&mut buf) {
            Ok(received) => println!("received {} bytes {:?}", received, &buf[..received]),
            Err(e) => panic!("recv function failed: {:?}", e),
        }
    }

    fn handle_socket_error(socket: &UdpSocket) {
        match socket.take_error() {
            Ok(Some(error)) => eprintln!("UdpSocket error: {:?}", error),
            Ok(None) => println!("No error"),
            Err(error) => eprintln!("UdpSocket.take_error failed: {:?}", error),
        }
    }

    #[test]
    fn udp_single_receiver_works_ipv4_stdnet() {
        let port = find_free_port();
        let ip = Ipv4Addr::new(127, 0, 0, 1);
        let socket_addr = SocketAddr::new(IpAddr::from(ip), port);
        let socket_addr2 = SocketAddr::new(IpAddr::from(ip), port + 1);

        let socket = UdpSocket::bind(&socket_addr).expect("couldn't bind to address");
        let socket2 = UdpSocket::bind(&socket_addr2).expect("couldn't bind to address");
        assert_eq!(socket.local_addr().unwrap(), socket_addr.clone());
        assert_eq!(socket2.local_addr().unwrap(), socket_addr2.clone());

        handle_socket_error(&socket);
        handle_socket_error(&socket2);

        socket
            .connect(socket_addr)
            .expect("connect function failed");
        socket2
            .connect(socket_addr2)
            .expect("connect function failed");
        assert_eq!(socket.peer_addr().unwrap(), socket_addr.clone());
        assert_eq!(socket2.peer_addr().unwrap(), socket_addr2.clone());

        socket.send(&[0; 10]).expect("couldn't send data");
        socket2.send(&[0; 10]).expect("couldn't send data");
        handle_socket_receive(&socket);
        handle_socket_receive(&socket2);
    }

    #[test]
    fn udp_multicast_works_ipv4_socket2() {
        let ip: IpAddr = Ipv4Addr::new(224, 0, 0, 123).into();
        let test = "single_listener";
        assert!(ip.is_multicast());
        let port = find_free_port();
        let addr = SocketAddr::new(ip, port);

        let client_done = Arc::new(AtomicBool::new(false));
        let notify = NotifyServer(Arc::clone(&client_done));

        multicast_listener(test, client_done, addr);

        // client test code send and receive code after here
        println!("{}:client: running", test);

        let message = b"Hello from client!";

        // create the sending socket
        let socket = new_sender(&addr).expect("could not create sender!");
        socket.send_to(message, &addr).expect("could not send_to!");

        let mut buf = [0u8; 64]; // receive buffer

        match socket.recv_from(&mut buf) {
            Ok((len, _remote_addr)) => {
                let data = &buf[..len];
                let response = String::from_utf8_lossy(data);

                println!("{}:client: got data: {}", test, response);

                // verify it's what we expected
                assert_eq!(test, response);
            }
            Err(err) => {
                println!("{}:client: had a problem: {}", test, err);
                assert!(false);
            }
        }

        // make sure we don't notify the server until the end of the client test
        drop(notify);
    }

    #[test]
    fn udp_multicast_multiple_listeners_works() {
        let ip: IpAddr = Ipv4Addr::new(224, 0, 0, 123).into();
        let test = "multi_listener";
        assert!(ip.is_multicast());
        let port = find_free_port();
        let addr = SocketAddr::new(ip, port);

        let client_done = Arc::new(AtomicBool::new(false));
        let client_done2 = Arc::new(AtomicBool::new(false));
        let client_done3 = Arc::new(AtomicBool::new(false));
        let notify = NotifyServer(Arc::clone(&client_done));

        multicast_listener(test, client_done, addr);
        multicast_listener(test, client_done2, addr);
        multicast_listener(test, client_done3, addr);
        // client test code send and receive code after here
        println!("{}:client: running", test);

        let message = b"Hello from client!";

        // create the sending socket
        let socket = new_sender(&addr).expect("could not create sender!");
        socket.send_to(message, &addr).expect("could not send_to!");

        let mut buf = [0u8; 64]; // receive buffer

        match socket.recv_from(&mut buf) {
            Ok((len, _remote_addr)) => {
                let data = &buf[..len];
                let response = String::from_utf8_lossy(data);

                println!("{}:client: got data: {}", test, response);

                // verify it's what we expected
                assert_eq!(test, response);
            }
            Err(err) => {
                println!("{}:client: had a problem: {}", test, err);
                assert!(false);
            }
        }

        // make sure we don't notify the server until the end of the client test
        drop(notify);
    }
}
