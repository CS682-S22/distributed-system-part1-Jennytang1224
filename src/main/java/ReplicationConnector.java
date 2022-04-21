import dsd.pubsub.protos.Acknowledgment;
import dsd.pubsub.protos.PeerInfo;

import java.util.HashMap;

/**
 * A class Connector for all brokers on replication
 */
class ReplicationConnector implements Runnable {
    int brokerCounter = 1; // 0 is LB
    Connection dataConnection;
    String hostName;
    int brokerID;
    int peerID;
    int dataPort;
    boolean isAlive = true;
    int numOfBrokers;
    HashMap<Integer, Connection> dataConnMap;
    MembershipTable membershipTable;

    public ReplicationConnector(String hostName, int dataPort, int numOfBrokers,
                                HashMap<Integer, Connection> dataConnMap, MembershipTable membershipTable) {
        this.hostName = hostName;
        this.dataPort = dataPort;
        this.brokerID = Utilities.getBrokerIDFromFile(hostName, String.valueOf(dataPort), Utilities.brokerConfigFile);
        this.numOfBrokers = numOfBrokers;
        this.dataConnMap = dataConnMap;
        this.membershipTable = membershipTable;
    }

    @Override
    public void run() {
        // create REPLICATION connections between all brokers
        while (brokerCounter <= numOfBrokers) {
            String peerHostName = Utilities.getHostnameByID(brokerCounter);
            int peerPort = Utilities.getPortRepByID(brokerCounter);
            peerID = Utilities.getBrokerIDFromFile(peerHostName, String.valueOf(peerPort), Utilities.brokerConfigFile);
            dataConnection = new Connection(peerHostName, peerPort, isAlive);
            System.out.println("made data connection with peer: " + peerHostName + ":" + peerPort);
            dataConnMap.put(brokerCounter, dataConnection); // add connection to map, {5:conn5, 4:conn4, 3:conn3}
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // send peer info to other brokers
            String type = "broker";
            PeerInfo.Peer peerInfo = PeerInfo.Peer.newBuilder()
                    .setType(type)
                    .setHostName(hostName)
                    .setPortNumber(dataPort)
                    .build();

            dataConnection.send(peerInfo.toByteArray());
            System.out.println("sent peer info to broker " + peerID + "  " + peerHostName + ":" + peerPort + "...");
            brokerCounter++;  // next broker in the map
        }
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //if im not the leader, ask leader for the newest data
        if(brokerID != membershipTable.getLeaderID()) {
            Acknowledgment.ack requestData = Acknowledgment.ack.newBuilder()
                    .setSenderType("catchup")
                    .setLeadBrokerLocation(String.valueOf(brokerID)) // my broker id
                    .build();
            dataConnMap.get(membershipTable.getLeaderID()).send(requestData.toByteArray());
            System.out.println("############sent request to leader to catch up all data");
        }
    }
}