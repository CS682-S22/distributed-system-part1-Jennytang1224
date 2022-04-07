
import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.HeartBeatMessage;
import dsd.pubsub.protos.PeerInfo;

import javax.sound.sampled.LineListener;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class LeaderBasedBroker {
    private String hostName;
    private int port;
    private ServerSocket serverSocket;
    private Socket socket;
    private DataInputStream input;
    private DataOutputStream output;
    private static volatile boolean running = true;
    static CopyOnWriteArrayList<byte[]> topicList;
    static Map<String, CopyOnWriteArrayList> topicMap;// <topic1: topic1_list, topic2: topic2_list>
    static Server server;
    private Connection connection;
    static String peerHostName;
    static int peerPort;
    static int messageCounter = 0;
    static int offsetInMem = 0;
    static int brokerID;
    private String brokerConfigFile = "files/brokerConfig.json";
    private static int numOfBrokers = 5;
    private int brokerCounter = 1;
    static HashMap<Integer, Connection> connMap = new HashMap<>();
    private static MembershipTable membershipTable = new MembershipTable();
    static boolean inElection = false;


    public LeaderBasedBroker(String hostName, int port) {
        this.hostName = hostName;
        this.port = port;
        this.topicList = new CopyOnWriteArrayList<>();
        this.topicMap = new HashMap<>();
        brokerID = Utilities.getBrokerIDFromFile(hostName, String.valueOf(port), "files/brokerConfig.json");
        System.out.println("~~~~~~~~~~~initial table:");
        membershipTable.print();
        System.out.println(" ");
    }


    //broker needs to constantly listen and
    // unpack proto buffer see if its producer or consumer connection, peerinfo

    /**
     * use threads to start the connections, receive and send data concurrently
     */
    public void run() throws IOException {
        // listening for incoming connection, could be load balancer/producer or other brokers
        Thread serverListener = new Thread(() -> {
            boolean running = true;
            try {
                this.server = new Server(this.port);
                System.out.println("broker start listening on port: " + this.port + "...");
            } catch (IOException e) {
                e.printStackTrace();
            }
            while (running) {
                // add broker to membership table
                Connection connection = this.server.nextConnection(); // calls accept on server socket to block
                Thread serverReceiver = new Thread(new Receiver(this.hostName, this.port, connection));
                serverReceiver.start();
            }
        });
        serverListener.start();

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // connection to all brokers and send peer info
        Thread connector = new Thread(new Connector(this.hostName, this.port));
        connector.start();

    }

    /**
     * inner class Receiver
     */
    static class Receiver implements Runnable {
        private String name;
        private int port;
        private Connection conn;
        boolean receiving = true;
        int counter = 0;
        private String type;

        public Receiver(String name, int port, Connection conn) {
            this.name = name;
            this.port = port;
            this.conn = conn;

        }

        @Override
        public void run() {
            PeerInfo.Peer p = null;
            while (receiving) {
                //   System.out.println("broker receiving data from connection ");
                byte[] buffer = conn.receive();
                if (buffer == null || buffer.length == 0) {
                    // System.out.println("nothing received/ finished receiving");
                } else {
                    if (counter == 0) { // first mesg is peerinfo
                        try {
                            p = PeerInfo.Peer.parseFrom(buffer);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }

                        type = p.getType(); // consumer or producer
                        System.out.println("\n *** New Connection coming in ***");
                        System.out.println("Peer type: " + type);
                        peerHostName = p.getHostName();
                        peerPort = p.getPortNumber();
                        int peerID = Utilities.getBrokerIDFromFile(peerHostName, String.valueOf(peerPort), "files/brokerConfig.json");


                        if (type.equals("producer")) { // only bc this broker is a leader
                            // get the messageInfo though socket
                            System.out.println("this Broker now has connected to PRODUCER: " + peerHostName + " port: " + peerPort + "\n");
                            counter++;

                        } else if (type.equals("broker")) {
                            System.out.println("this broker NOW has connected to BROKER: " + peerHostName + " port: " + peerPort + "\n");
                            counter++;



                        }

                    } else { // when receiving producer data
                        if (type.equals("producer")) {
                            Thread th = new Thread(new LeaderBasedReceiveProducerData(conn, buffer, topicMap, messageCounter, offsetInMem));
                            th.start();
                            try {
                                th.join();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            counter++;
                            messageCounter++;
                        } else if (type.equals("broker")) {
                            //if its heartbeat
                            HeartBeatMessage.HeartBeat heartBeat = null;
                            try {
                                heartBeat = HeartBeatMessage.HeartBeat.parseFrom(buffer);
                            } catch (InvalidProtocolBufferException e) {
                                e.printStackTrace();
                            }
                            if(heartBeat !=null) {
                                int senderId = heartBeat.getSenderID();
                                System.out.println("receiving heartbeat from broker " + senderId + "...");

                                // receive heartbeat from broker and response with its own id
                                HeartBeatMessage.HeartBeat response = HeartBeatMessage.HeartBeat.newBuilder()
                                        .setSenderID(brokerID)
                                        .setNumOfRetires(0)
                                        .build();
                                conn.send(response.toByteArray());
                            }

//                             //check if this send is leader, replicate data other brokers
//                            Thread th = new Thread(new LeaderBasedSendConsumerData(conn, buffer, topicMap));
//                            th.start();
//                            try {
//                                th.join();
//                            } catch (InterruptedException e) {
//                                e.printStackTrace();
//                            }
                            counter++;

                        } else {
                            System.out.println("invalid type, should be either producer or consumer");
                            // System.exit(-1);
                        }
                    }
                }
            }
        }
    }


    /**
     * inner class Sender
     */
    static class HeartBeatSender extends TimerTask implements Runnable {
        private String name;
        private String port;
        private Connection conn;
        boolean sending = true;
        int brokerID;
        int retires = 3;
        private CS601BlockingQueue<HeartBeatMessage.HeartBeat> bq;
        private ExecutorService executor;
        private String peerHostName;
        private int peerPort;
        private int peerID;
        private HashMap<Integer, Connection> connMap;
        // int retryCount = 1;

        public HeartBeatSender(String name, String port, Connection conn, String peerHostName, int peerPort, HashMap<Integer, Connection> connMap) {
            this.name = name;
            this.port = port;
            this.brokerID = Utilities.getBrokerIDFromFile(name, String.valueOf(port), "files/brokerConfig.json");
            this.conn = conn;
            this.peerHostName = peerHostName;
            this.peerPort = peerPort;
            this.peerID = Utilities.getBrokerIDFromFile(peerHostName, String.valueOf(peerPort), "files/brokerConfig.json");
            this.bq = new CS601BlockingQueue<>(1);
            this.executor = Executors.newSingleThreadExecutor();
            this.connMap = connMap;
        }

        @Override
        public void run() {
            //constantly sending heart beat to other brokers
            HeartBeatMessage.HeartBeat f;
            Runnable add = () -> {
                try {
                    byte[] result = conn.receive();
                    if (result != null) {
                        bq.put(HeartBeatMessage.HeartBeat.parseFrom(result));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            };

            while (sending) {
                HeartBeatMessage.HeartBeat heartBeatMessage = HeartBeatMessage.HeartBeat.newBuilder()
                        .setSenderID(brokerID)
                        .setNumOfRetires(0).build();
                //  System.out.println("num of retires: " + retryCount);
                // retryCount++;
                //send heartbeat msg
                conn.send(heartBeatMessage.toByteArray());

                // timeout for heartbeat response
                executor.execute(add);
                int replyingBrokerId = -1;
                f = bq.poll(1000 * retires);
                if (f != null) { // received a pack within in timeout, send a new heartbeat
                    replyingBrokerId = f.getSenderID();
//                    if(replyingBrokerId != peerID){
//                        System.out.println("!!!!! receiving from different peer");
//                    }
                    System.out.println("received heartbeat response from peer: " + replyingBrokerId);
                    // retryCount = 1; // reset

                } else { // not receive response within timeout
                    // if more than number of retires, update table as the broker failed, and stop sending
                    System.out.println("exceed timeout, assume peer: " + peerID + " is dead ");
                    //remove broker from the table
                    if (membershipTable.getMemberInfo(peerID).isLeader) { // leader is dead
                        // bully election .. need another class
//                        inElection = true;
//                        Thread th = new Thread(new BullyElection(brokerID, membershipTable, connMap)); // oldLeader, membershipTable
//                        th.start();
//                        try {
//                            th.join();
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }

                          membershipTable.switchLeaderShip(peerID, peerID-1); // naively choosing next smallest id, change later
                    } else { //follower is dead, mark dead
                        membershipTable.markDead(peerID);
                    }
                    System.out.println("~~~~~~~~~~~~~~~~~~table after failed to return heartbeat msg");
                    membershipTable.print();
                    System.out.println(" ");


                    sending = false;
                    break;

                    // else if within num of retires, send same heart beat again, go back to while loop
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }


            }
        }
    }


    /**
     * inner class Connector for all brokers
     */
    static class Connector implements Runnable {
        int brokerCounter = 5;
        private String brokerConfigFile = "files/brokerConfig.json";
        private Socket socket;
        private DataInputStream input;
        private DataOutputStream output;
        Connection connection;
        String hostName;
        int port;
        int brokerID;
        int peerID;

        public Connector(String hostName, int port) {
            this.hostName = hostName;
            this.port = port;
            this.brokerID = Utilities.getBrokerIDFromFile(hostName, String.valueOf(port), "files/brokerConfig.json");

        }

        @Override
        public void run() {
            // create connections to all other lower brokers than itself
            while (brokerCounter > 0) { // need to generalize
                List<Object> maps = Utilities.readBrokerConfig(brokerConfigFile);
                IPMap ipMap = (IPMap) maps.get(0);
                PortMap portMap = (PortMap) maps.get(1);
                String peerHostName = ipMap.getIpById(String.valueOf(brokerCounter));
                int peerPort = Integer.parseInt(portMap.getPortById(String.valueOf(brokerCounter)));
                peerID = Utilities.getBrokerIDFromFile(peerHostName, String.valueOf(peerPort), "files/brokerConfig.json");

                //   if (brokerCounter < brokerID) {
 //               try {
//                    this.socket = new Socket(peerHostName, peerPort);
//                    this.connection = new Connection(this.socket);
                    connection = new Connection(peerHostName, peerPort); // connection to other peer
//                } catch (IOException e) {
//                    // e.printStackTrace();
//                }


                //add this broker to membership table
                boolean isLeader = false;
                if (peerID == numOfBrokers) {//leader
                    isLeader = true;
                }
                MemberInfo memberInfo = new MemberInfo(peerHostName, peerPort, "", isLeader, true);

                if (membershipTable.size() != 0) {
                    membershipTable.put(peerID, memberInfo);
                } else {
                    membershipTable.put(peerID, memberInfo);
                }
                System.out.println("~~~~~~~~~~~~~~~~ after broker " + peerID + " connected..");
                membershipTable.print();
                System.out.println(" ");

                    connMap.put(brokerCounter, connection); // add connection to map, {5:conn5, 4:conn4, 3:conn3}
                    System.out.println("Connected to broker: " + peerHostName + ":" + peerPort);

                    // send peer info to other brokers
                    String type = "broker";
                    PeerInfo.Peer peerInfo = PeerInfo.Peer.newBuilder()
                            .setType(type)
                            .setHostName(hostName)
                            .setPortNumber(port)
                            .build();
                    if (connection == null) {
                        System.out.println("(This broker is NOT in use)");
                        return;
                    }
                    connection.send(peerInfo.toByteArray());
                    System.out.println("sent peer info to broker " + peerID + "...");

                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    // send heartbeat to others
                    System.out.println("Now sending heartbeat to " + peerID + "...");
                    Thread heartbeatSender = new Thread(new HeartBeatSender(this.hostName, String.valueOf(this.port), connection, peerHostName, peerPort, connMap));
                    heartbeatSender.start();


                brokerCounter--;  // next broker in the map
            }


        }

    }
}
