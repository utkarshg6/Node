// Copyright (c) 2019, MASQ (https://masq.ai) and/or its affiliates. All rights reserved.

use crate::sub_lib::neighborhood::NodeDescriptor;
use openssl::init;
use std::collections::HashSet;

#[derive(PartialEq, Debug)]
enum ConnectionStageErrors {
    TcpConnectionFailed,
    NoGossipResponseReceived,
}

#[derive(PartialEq, Debug)]
enum ConnectionStage {
    StageZero,
    TcpConnectionEstablished,
    NeighborshipEstablished,
    Failed(ConnectionStageErrors),
}

#[derive(PartialEq, Debug)]
pub struct ConnectionProgress {
    pub starting_descriptor: NodeDescriptor,
    current_descriptor: NodeDescriptor,
    connection_stage: ConnectionStage,
    previous_pass_targets: HashSet<NodeDescriptor>,
    // Uses:
    // Stop the cycle if we receive a node descriptor that is a part of this hash set.
}

// NodeConnection 1 ->  Desc 1 => TCPConnectionError
// NodeConnection 2 ->  Desc 2 => 10 Pass Gossips Vec[11]
// NodeConnection 3 ->  Desc 3

// Initiating TCP Connection for Node Descriptor 0x00
// TCPConn Established for Node Descriptor 0x00
// Received 1 Pass Gossip. Passed to Node Descriptor 0x01
// Received 2 Pass Gossip Passed to Node Descriptor 0x02
// NeighborshipEstablished with Node Descriptor 0x02
// Three Hops Route Found. You can relay data.

// Initiating TCP Connection for Node Descriptor 0x00
// TCPConn Established for Node Descriptor 0x00
// Sending Debut Gossip to Node Descriptor 0x00.
// Received 1 Pass Gossip. Passed to Node Descriptor 0x01
// Sending Debut Gossip to Node Descriptor 0x01.
// Received 2 Pass Gossip Passed to Node Descriptor 0x02
// Sending Debut Gossip to Node Descriptor 0x02.
// IntroductionGossip Received from Node Descriptor 0x02. New Node 0x03 Introduced.
// NeighborshipEstablished with Node Descriptor 0x02
// Three Hops Route Found. You can relay data.

#[derive(PartialEq, Debug)]
enum OverallConnectionStage {
    NotConnected,        // Not connected to any neighbor.
    ConnectedToNeighbor, // Neighborship is established. Same as No 3 hops route found.
    ThreeHopsRouteFound, // check_connectedness() returned true, data can now be relayed.
}

// TODO: Migrate this struct and code related to it to a new module and make that module public only for neighborhood
#[derive(PartialEq, Debug)]
pub struct OverallConnectionStatus {
    can_make_routes: bool, // Boolean flag which becomes true iff three hops route was found.
    stage: OverallConnectionStage, // Stores one out of the three stages, mentioned in enum.
    progress: Vec<ConnectionProgress>, // Stores the info of individual NodeConnection, each element might be corresponding to the descriptors entered by user.
}

impl OverallConnectionStatus {
    pub fn new(initial_node_descriptors: Vec<NodeDescriptor>) -> Self {
        // todo!("Construct the OverallConnectionStatus");
        let progress = initial_node_descriptors
            .iter()
            .map(|node_descriptor| ConnectionProgress {
                starting_descriptor: node_descriptor.clone(),
                current_descriptor: node_descriptor.clone(),
                connection_stage: ConnectionStage::StageZero,
                previous_pass_targets: HashSet::new(),
            })
            .collect();

        Self {
            can_make_routes: false,
            stage: OverallConnectionStage::NotConnected,
            progress,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &NodeDescriptor> {
        self.progress
            .iter()
            .map(|connection_progress| &connection_progress.starting_descriptor)
    }

    pub fn is_empty(&self) -> bool {
        self.progress.is_empty()
    }

    pub fn remove(&mut self, index: usize) -> NodeDescriptor {
        let removed_desc = self.progress[index].starting_descriptor.clone();
        self.progress.remove(index);
        removed_desc
    }

    // fn get_connected_neighbors() {
    //     todo!("Fetch the connected neighbors from the Neighborhood Database")
    // }
}

//
// Edge 1 ->
// Explanation:     Introduction Gossip is received.
// Log:             Connected to new neighbor(s) - {desc_1}, {desc_2}, .. {desc_n}.
//
// Edge 2 ->
// Explanation:     The `check_connectedness()` returns false, still connected to neighbor(s).
// Log:             Attempt to search for Three Hops Route: Not found, retrying...
//
// Edge 3 ->
// Explanation:     The `check_connectedness()` returns true, hence three hops route found.
// Log:             Attempt to search for Three Hops Route: Route found. Data can be relayed.
//
// Edge 4 ->
// Explanation:     Some nodes in the network died. Hence, three hops route was lost.
// Log:             Three hops route lost.
//
// Edge 5 ->
// Explanation:     Lost connection with some node(s), but still connected to 1 node.
// Log:             Lost connection with node {desc}.
//
// Edge 6 ->
// Explanation:     Uncertain reason due to which lost connection to each and every node.
// Log:             Lost connection with all the nodes.

// Some Steps to follow ==>
// 1. Increase the count for Stage Zero
// 2. Initiate a TCP Connection. OK() -> TcpConnectionEstablished, Err() -> Failed and throw TcpConnectionFailed
// 3. Send a Debut Gossip
// 4. Waiting Period. IntroductionGossip -> Move to Next Step,
//    PassGossip -> Update the NodeConnection and retry the whole process,
//    TimeOut -> Failed and throw NoResponseReceived
// 5. Check for check_connectedness(), true -> Fully Connected, false -> Not able to Route

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::main_cryptde;

    #[test]
    fn able_to_create_overall_connection_status() {
        let node_desc_1 = NodeDescriptor::try_from((
            main_cryptde(), // Used to provide default cryptde
            "masq://eth-ropsten:AQIDBA@1.2.3.4:1234/2345",
        ))
        .unwrap();
        let node_desc_2 = NodeDescriptor::try_from((
            main_cryptde(),
            "masq://eth-ropsten:AgMEBQ@1.2.3.5:1234/2345",
        ))
        .unwrap();
        let initial_node_descriptors = vec![node_desc_1.clone(), node_desc_2.clone()];

        let subject = OverallConnectionStatus::new(initial_node_descriptors);

        assert_eq!(
            subject,
            OverallConnectionStatus {
                can_make_routes: false,
                stage: OverallConnectionStage::NotConnected,
                progress: vec![
                    ConnectionProgress {
                        starting_descriptor: node_desc_1.clone(),
                        current_descriptor: node_desc_1,
                        connection_stage: ConnectionStage::StageZero,
                        previous_pass_targets: HashSet::new()
                    },
                    ConnectionProgress {
                        starting_descriptor: node_desc_2.clone(),
                        current_descriptor: node_desc_2,
                        connection_stage: ConnectionStage::StageZero,
                        previous_pass_targets: HashSet::new()
                    }
                ]
            }
        );
    }

    #[test]
    fn overall_connection_status_identifies_as_empty() {
        let subject = OverallConnectionStatus::new(vec![]);

        assert_eq!(subject.is_empty(), true);
    }

    #[test]
    fn overall_connection_status_identifies_as_non_empty() {
        let node_desc = NodeDescriptor::try_from((
            main_cryptde(),
            "masq://eth-ropsten:AQIDBA@1.2.3.4:1234/2345",
        ))
        .unwrap();

        let initial_node_descriptors = vec![node_desc.clone()];

        let subject = OverallConnectionStatus::new(initial_node_descriptors);

        assert_eq!(subject.is_empty(), false);
    }

    #[test]
    fn overall_connection_status_is_iterable() {
        let node_desc_1 = NodeDescriptor::try_from((
            main_cryptde(),
            "masq://eth-ropsten:AQIDBA@1.2.3.4:1234/2345",
        ))
        .unwrap();
        let node_desc_2 = NodeDescriptor::try_from((
            main_cryptde(),
            "masq://eth-ropsten:AgMEBQ@1.2.3.5:1234/2345",
        ))
        .unwrap();
        let initial_node_descriptors = vec![node_desc_1.clone(), node_desc_2.clone()];
        let mut subject = OverallConnectionStatus::new(initial_node_descriptors);

        let mut result = subject.iter();

        assert_eq!(result.next(), Some(&node_desc_1));
        assert_eq!(result.next(), Some(&node_desc_2));
        assert_eq!(result.next(), None);
    }

    #[test]
    fn remove_deletes_descriptor_s_progress_and_returns_node_descriptor() {
        let node_desc_1 = NodeDescriptor::try_from((
            main_cryptde(),
            "masq://eth-ropsten:AQIDBA@1.2.3.4:1234/2345",
        ))
        .unwrap();
        let node_desc_2 = NodeDescriptor::try_from((
            main_cryptde(),
            "masq://eth-ropsten:AgMEBQ@1.2.3.5:1234/2345",
        ))
        .unwrap();
        let initial_node_descriptors = vec![node_desc_1.clone(), node_desc_2.clone()];
        let mut subject = OverallConnectionStatus::new(initial_node_descriptors);

        let removed_desc_1 = subject.remove(0);
        let removed_desc_2 = subject.remove(0);

        assert_eq!(removed_desc_1, node_desc_1);
        assert_eq!(removed_desc_2, node_desc_2);
        assert_eq!(subject, OverallConnectionStatus::new(vec![]));
    }
}
