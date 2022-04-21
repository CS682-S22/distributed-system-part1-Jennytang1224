import dsd.pubsub.protos.PeerInfo;
import java.util.HashMap;

/**
 * inner class Connector for all brokers
 */
class HeatBeatConnector implements Runnable {
    int brokerCounter = 0; // 0 is LB
    Connection connection;
    String hostName;
    int port;
    int brokerID;
    int peerID;
    boolean isAlive = true;
    boolean isLeader = false;
    boolean isLB = false;
    int currentLeader;
    private static int numOfBrokers;
    static HashMap<Integer, Connection> connMap;
    MembershipTable membershipTable;
    boolean inElection;

    public HeatBeatConnector(String hostName, int port, int numOfBrokers,
                             HashMap<Integer, Connection> connMap, MembershipTable membershipTable, boolean inElection){
        this.hostName = hostName;
        this.port = port;
        this.brokerID = Utilities.getBrokerIDFromFile(hostName, String.valueOf(port), Utilities.brokerConfigFile);
        this.numOfBrokers = numOfBrokers;
        this.connMap = connMap;
        this.membershipTable = membershipTable;
        this.inElection = inElection;
    }

    @Override
    public void run() {
        // create connections to all other lower brokers than itself
        while (brokerCounter <= numOfBrokers) { // need to generalize
            String peerHostName = Utilities.getHostnameByID(brokerCounter);
            int peerPort = Utilities.getPortByID(brokerCounter);
            peerID = Utilities.getBrokerIDFromFile(peerHostName, String.valueOf(peerPort),  Utilities.brokerConfigFile);
            if (brokerID == 1) {
                currentLeader = 1;
            }
            isLB = peerID == 0;
            isLeader = peerID == 1;
            isAlive = true;
            connection = new Connection(peerHostName, peerPort, isAlive); // make connection to peers in config
            connMap.put(brokerCounter, connection); // add connection to map, {5:conn5, 4:conn4, 3:conn3}

            isAlive = connection.getAlive();
            if (connection == null) {
                System.out.println("(This broker is NOT in use)");
                return;
            }

            if (currentLeader == brokerID && isLB) {
                System.out.println("Connected to LB : " + peerHostName + ":" + peerPort);
                Utilities.leaderConnectToLB(peerHostName, peerPort, hostName, port, connection);
                System.out.println("Lead broker sent peer info to Load Balancer ... \n");
            }

            if (brokerCounter > 0) { // brokers
                System.out.println("Connected to broker : " + peerHostName + ":" + peerPort);
                MemberInfo memberInfo = new MemberInfo(peerHostName, peerPort, "", isLeader, isAlive);
                membershipTable.put(peerID, memberInfo);
                System.out.println("~~~~~~~~~~~~~~~~ after broker " + peerID + " connected..");
                membershipTable.print();
                System.out.println(" ");

                // if im a leader, send membership updates to LB
                if (membershipTable.membershipTable.containsKey(brokerID) && membershipTable.getMemberInfo(brokerID).isLeader) {
                    Utilities.sendMembershipTableUpdates(connMap.get(0), "new", brokerID, peerID,
                            peerHostName, peerPort, "", isLeader, isAlive);
                    membershipTable.print();
                }

                // send peer info to other brokers
                String type = "broker";
                PeerInfo.Peer peerInfo = PeerInfo.Peer.newBuilder()
                        .setType(type)
                        .setHostName(hostName)
                        .setPortNumber(port)
                        .build();

                connection.send(peerInfo.toByteArray());
            }
            brokerCounter++;  // next broker in the map
            currentLeader = membershipTable.getLeaderID();
        }

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for(int i = 1; i <= numOfBrokers; i++){
            MemberInfo m = membershipTable.getMemberInfo(i);
            String peerHost = m.getHostName();
            int peerPortNum = m.getPort();
            //sending heartbeat to brokers
            if (m.isAlive) {
                System.out.println("Now sending heartbeat to " + i + "...");
                HeartBeatSender sender = new HeartBeatSender(this.hostName,
                        String.valueOf(this.port), connMap.get(i), peerHost, peerPortNum,
                        connMap, membershipTable, inElection);
                Thread heartbeatSender = new Thread(sender);
                heartbeatSender.start();
            }

        }
    }
}
