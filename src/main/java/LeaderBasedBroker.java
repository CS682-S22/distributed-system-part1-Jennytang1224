

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.*;

import javax.xml.crypto.dom.DOMCryptoContext;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
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
    //static int brokerID;
    private String brokerConfigFile = "files/brokerConfig.json";
    private static int numOfBrokers = 5;
    private int brokerCounter = 1;
    static HashMap<Integer, Connection> connMap = new HashMap<>();
    private static MembershipTable membershipTable = new MembershipTable();
    static volatile boolean inElection = false;
    static int currentLeader = 1;
    private static ExecutorService executor;
    int dataPort;
    static Server dataServer;
    static HashMap<Integer, Connection> dataConnMap = new HashMap<>();
    static int dataCounter = 0;
    static boolean synchronous = true;
    static volatile int ackCount = 0;
    static Connection connWithProducer;



    public LeaderBasedBroker(String hostName, int port, int dataPort) {
        this.hostName = hostName;
        this.port = port;
        this.topicList = new CopyOnWriteArrayList<>();
        this.topicMap = new HashMap<>();
        this.executor = Executors.newSingleThreadExecutor();
        this.dataPort = dataPort;
    }


    //broker needs to constantly listen and
    // unpack proto buffer see if its producer or consumer connection, peerinfo

    /**
     * use threads to start the connections, receive and send data concurrently
     */
    public void run() throws IOException {
        // listening for incoming connection, could be lead broker sending data over
        Thread dataServerListener = new Thread(() -> {
            boolean running = true;
            try {
                this.dataServer = new Server(this.dataPort);
                System.out.println("broker start listening on data port: " + this.dataPort + "...");
            } catch (IOException e) {
                e.printStackTrace();
            }
            while (running) {
                Connection dataConnection = this.dataServer.nextConnection(); // calls accept on server socket to block
                Thread dataServerReceiver = new Thread(new DataReceiver(this.hostName, this.dataPort, dataConnection));
                dataServerReceiver.start();
            }
        });
        dataServerListener.start();


        try {
            Thread.sleep(8000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //connect to other brokers for replication purpose
        Thread replicationConnector = new Thread(new ReplicationConnector(this.hostName, this.dataPort));
        replicationConnector.start();

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
                Connection connection = this.server.nextConnection(); // calls accept on server socket to block
                Thread serverReceiver = new Thread(new Receiver(this.hostName, this.port, connection));
                serverReceiver.start();
            }
        });
        serverListener.start();

        try {
            Thread.sleep(8000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // connection to all brokers and send peer info
        Thread heartBeatConnector = new Thread(new HeatBeatConnector(this.hostName, this.port));
        heartBeatConnector.start();
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
        int brokerID;
        int peerID;

        public Receiver(String name, int port, Connection conn) {
            this.name = name;
            this.port = port;
            this.conn = conn;
            brokerID = Utilities.getBrokerIDFromFile(name, String.valueOf(port), "files/brokerConfig.json");
        }

        @Override
        public void run() {
            PeerInfo.Peer p = null;
            while (receiving) {
                byte[] buffer = conn.receive();
                if (buffer == null || buffer.length == 0) {
                }
                else {
                    if (counter == 0) { // first msg is peerinfo
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
                        peerID = Utilities.getBrokerIDFromFile(peerHostName, String.valueOf(peerPort), "files/brokerConfig.json");

                        if (type.equals("producer")) { // hear from producer only bc this broker is a leader
                            System.out.println("this Broker now has connected to PRODUCER: " + peerHostName + " port: " + peerPort + "\n");
                            counter++;
                            connWithProducer = conn;
                        }

                        else if (type.equals("broker")) { // hear from other brokers: heartbeat/election
                            System.out.println("this broker NOW has connected to BROKER: " + peerHostName + " port: " + peerPort + "\n");
                            counter++;
                        }
                        else if (type.equals("consumer")) { // hear from producer/LB only bc this broker is a leader
                            System.out.println("this Broker now has connected to LB(consumer): " + peerHostName + " port: " + peerPort + "\n");
                            counter++;
                        }

                    }

                    else {
                        if (type.equals("producer")) {  // when receiving data from LB/producer
                            System.out.println("!!!!!!!!! receiving data from producer...");
                            synchronized (this) {
                                //save data to my topic map:
                                Thread th = new Thread(new LeaderBasedReceiveProducerData(conn, ByteString.copyFrom(buffer), topicMap, messageCounter, offsetInMem));
                                th.start();
                                try {
                                    th.join();
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }

                                //replications:
                                if(!synchronous) { //replication with Asynchronous followers
                                    AsynchronousReplication asynchronousReplication = new AsynchronousReplication(membershipTable, buffer, brokerID, dataConnMap);//use data connections
                                 //   replication.run();
                                    Thread rep = new Thread(asynchronousReplication);
                                    rep.start();
                                }else{ //replication with Synchronous followers
                                    //need to send ack
                                    SynchronousReplication synchronousReplication = new SynchronousReplication(membershipTable, buffer, brokerID, dataConnMap);//use data connections
                                    //   replication.run();
                                    Thread rep = new Thread(synchronousReplication);
                                    rep.start();
                                    try { // to get tha ack count
                                        Thread.sleep(100);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                    System.out.println("aCK COUNT:" + ackCount + " num needed: " + synchronousReplication.numOfAckNeeded);
                                    if(ackCount == synchronousReplication.numOfAckNeeded){//all followers get replica
                                        //send producer a big ack for next data
                                        System.out.println("sending big ack to producer");
                                        Acknowledgment.ack ackToProducer = Acknowledgment.ack.newBuilder()
                                                .setSenderType("leadBroker")
                                                .build();
                                        connWithProducer.send(ackToProducer.toByteArray());
                                    }

                                }
                            }
                            counter++;
                            messageCounter++;
                        }

                        else if (type.equals("consumer")){
                            System.out.println("receiving consumer request from LB...");
                            Thread th = new Thread(new LeaderBasedSendConsumerData(connMap.get(0), buffer, topicMap));
                            th.start();
                            try {
                                th.join();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            counter++;

                        }
                        else if (type.equals("broker")) {// when receiving msg from broker
                            // other send me heartbeat msg(me reply) / election(other inform new leader, me update table)/ election(other send election msg to me, needs reply to other broker )
                            Resp.Response f = null;
                            try {
                                f = Resp.Response.parseFrom(buffer);
                            } catch (InvalidProtocolBufferException exception) {
                                exception.printStackTrace();
                            }
                            if (f != null) {
                               // System.out.println("type in broker: " + f.getType());
                                if (f.getType().equals("heartbeat")) {
                                    inElection = false;
                                } else if (f.getType().equals("election")) {
                                    inElection = true;
                                } else {
                                    System.out.println("wrong type");
                                }

                                if (!inElection) { // if its heartbeat, me reply
                                    int senderId = f.getSenderID();
                                  //  System.out.println("broker " + brokerID + " receiving heartbeat from broker " + senderId + "...");

                                    // receive heartbeat from broker and response with its own id
                                    Resp.Response heartBeatResponse = Resp.Response.newBuilder()
                                            .setType("heartbeat").setSenderID(brokerID).build();
                                    conn.send(heartBeatResponse.toByteArray());
                                    try {
                                        Thread.sleep(300);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                  //  membershipTable.getMemberInfo(senderId).setAlive(true);

                                } else if (inElection && !f.getType().equals("heartbeat")) { // if election msg
                                    int senderId = f.getSenderID();
                                    int newLeader = f.getWinnerID();
                                    System.out.println(" -> > > receiving election msg from peer " + senderId);
                                    if (newLeader != -1) {//if other inform me new leader, me updated table
                                        System.out.println("new leader id:" + newLeader);

                                        int oldLeader = membershipTable.getLeaderID();
                                        if (oldLeader != -1) { // there's a leader
                                            System.out.println("in my table updated the new leader to " + newLeader);
                                            membershipTable.switchLeaderShip(oldLeader, newLeader);//update new leader
                                        } else {
                                            System.out.println("weird ... no current leader right now");
                                        }
                                        membershipTable.getMemberInfo(senderId).setAlive(true);
                                        membershipTable.print();

                                        //if this broker is leader, send table to load balancer
                                        if (membershipTable.getMemberInfo(brokerID).isLeader) {
                                            //get new leader hostname and port
                                            String peerHostName = Utilities.getHostnameByID(newLeader);
                                            int peerPort = Utilities.getPortByID(newLeader);
                                            //get LB hostname and port
                                            String LBHostName = Utilities.getHostnameByID(0);
                                            int LBPort = Utilities.getPortByID(0);
                                            Connection connLB = new Connection(LBHostName, LBPort, true); // make connection to peers in config
                                            Utilities.leaderConnectToLB(LBHostName, LBPort, peerHostName, peerPort, connLB);
//
//                                                try { // every 3 sec request new data
//                                                    Thread.sleep(1000);
//                                                } catch (InterruptedException e) {
//                                                    e.printStackTrace();
//                                                }

                                            // set new leadership
                                            Utilities.sendMembershipTableUpdates(connLB, "updateLeader", brokerID, newLeader,
                                                    "", 0, "", true, true);
                                            //cancel old leader's leadership
                                            Utilities.sendMembershipTableUpdates(connLB, "updateLeader", brokerID, currentLeader,
                                                    "", 0, "", false, false);
                                            //send table to LB
                                            Utilities.sendMembershipTableUpdates(connLB, "updateAlive", brokerID, senderId,
                                                    "", 0, "", membershipTable.getMemberInfo(senderId).isLeader, true);

                                        }
                                        currentLeader = newLeader;

                                        System.out.println("!!!!!!!!! election ended on broker " + brokerID + " side!");
                                        inElection = false;

                                        Resp.Response heartBeatMessage = Resp.Response.newBuilder().setType("heartbeat").setSenderID(brokerID).build();
                                        conn.send(heartBeatMessage.toByteArray());

                                    } else { //other sends election msg to me, me needs reply to other broker
                                        Resp.Response electionResponse = Resp.Response.newBuilder().setType("election")
                                                .setSenderID(brokerID).setWinnerID(-1).build();
                                        conn.send(electionResponse.toByteArray());
                                        inElection = true;
                                        System.out.println(" -> > >broker " + brokerID + " reply to broker " + senderId + " election msg...");
                                    }
                                }
                            }
                        }
                        counter++;
                    }
                }
            }
        }
    }



    /**
     * inner class Receiver
     */
    static class DataReceiver implements Runnable {
        private String name;
        private int port;
        private Connection conn;
        boolean receiving = true;
        int counter = 0;
        private String type;
        int brokerID;
        int peerID;

        public DataReceiver(String name, int port, Connection conn) {
            this.name = name;
            this.port = port;
            this.conn = conn;
            brokerID = Utilities.getBrokerIDFromFile(name, String.valueOf(port), "files/brokerConfig.json");
        }

        @Override
        public void run() {
            PeerInfo.Peer p = null;
            MessageInfo.Message f = null;
            while (receiving) {
                byte[] buffer = conn.receive();
                if (buffer == null || buffer.length == 0) {
                }
                else { // buffer not null
                    if (counter == 0) { // first msg is peerinfo
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
                        peerID = Utilities.getBrokerIDFromFile(peerHostName, String.valueOf(peerPort), "files/brokerConfig.json");
                     //   System.out.println("~~~~~~~    " + peerHostName + ":" + peerPort + " " + peerID );
                        if (type.equals("broker")) { // other brokers
                            System.out.println("this broker on data port has connected to BROKER " + peerHostName + ":" + peerPort + "\n");
                            counter++;
                        }

                    } else { // when receiving leader data
                        if (type.equals("broker")) { //leader sending data copy
                          //  System.out.println( brokerID + " received replica from lead broker ");
//                            try {
//                                f = MessageInfo.Message.parseFrom(buffer);
//                            } catch (InvalidProtocolBufferException e) {
//                                e.printStackTrace();
//                            }
                            Acknowledgment.ack m = null;
                            try{
                                m = Acknowledgment.ack.parseFrom(buffer);
                            } catch (InvalidProtocolBufferException e) {
                                e.printStackTrace();
                            }
                            if ( m != null) {
                                if(m.getSenderType().equals("data")){
                                    System.out.println(">>> Follower " + brokerID + " received data replication !!!");

                                    //send ack back to leader:
                                    Acknowledgment.ack ack = Acknowledgment.ack.newBuilder()
                                            .setSenderType("ack").build();

                                    dataConnMap.get(1).send(ack.toByteArray());
                                    System.out.println(">>> sent ack back to the lead broker!!!!!!");

                                    //store data
                                    ByteString dataInBytes = m.getData();
                                    Thread th = new Thread(new LeaderBasedReceiveProducerData(conn, dataInBytes, topicMap, dataCounter, offsetInMem));
                                    th.start();
                                    try {
                                        th.join();
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }

                                } else if(m.getSenderType().equals("ack")){ //only lead broker will receive ack
                                    System.out.println("~~~~~~~~~AN ACK received ");
                                    ackCount++;
                                }

                                dataCounter++;
                                counter++;
                            }
                        }
                    }
                }
            }
        }
    }


    /**
     * inner class Connector for all brokers
     */
    static class HeatBeatConnector implements Runnable {
        int brokerCounter = 0; // 0 is LB
        private String brokerConfigFile = "files/brokerConfig.json";
        Connection connection;
        String hostName;
        int port;
        int brokerID;
        int peerID;
        boolean isAlive = true;
        boolean isLeader = false;
        boolean isLB = false;
        int currentLeader;

        public HeatBeatConnector(String hostName, int port) {
            this.hostName = hostName;
            this.port = port;
            this.brokerID = Utilities.getBrokerIDFromFile(hostName, String.valueOf(port), "files/brokerConfig.json");
        }

        @Override
        public void run() {
            // create connections to all other lower brokers than itself
            while (brokerCounter <= numOfBrokers) { // need to generalize
                String peerHostName = Utilities.getHostnameByID(brokerCounter);
                int peerPort = Utilities.getPortByID(brokerCounter);
                peerID = Utilities.getBrokerIDFromFile(peerHostName, String.valueOf(peerPort), "files/brokerConfig.json");
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

                    if (membershipTable.size() != 0) {
                        membershipTable.put(peerID, memberInfo);
                    } else {
                        membershipTable.put(peerID, memberInfo);
                    }
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
                  //  System.out.println("sent peer info to broker " + peerID + "...\n");

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    //sending heartbeat to brokers
                    if (isAlive) {
                        System.out.println("Now sending heartbeat to " + peerID + "...\n");
                        HeartBeatSender sender = new HeartBeatSender(this.hostName,
                                String.valueOf(this.port), connection, peerHostName, peerPort,
                                connMap, membershipTable, inElection);
                        Thread heartbeatSender = new Thread(sender);
                        heartbeatSender.start();
                    }
                }

                brokerCounter++;  // next broker in the map
                currentLeader = membershipTable.getLeaderID();
            }
        }
    }



    /**
     * inner class Connector for all brokers
     */
    static class ReplicationConnector implements Runnable {
        int brokerCounter = 1; // 0 is LB
        private String brokerConfigFile = "files/brokerConfig.json";
        Connection dataConnection;
        String hostName;
        int port;
        int brokerID;
        int peerID;
        int currentLeader;
        int dataPort;
        boolean isAlive = true;

        public ReplicationConnector(String hostName, int dataPort) {
            this.hostName = hostName;
            this.dataPort = dataPort;
            this.brokerID = Utilities.getBrokerIDFromFile(hostName, String.valueOf(dataPort), "files/brokerConfig.json");
        }

        @Override
        public void run() {
            // create REPLICATION connections between all brokers
            while (brokerCounter <= numOfBrokers) {
                String peerHostName = Utilities.getHostnameByID(brokerCounter);
                int peerPort = Utilities.getPortRepByID(brokerCounter);
                peerID = Utilities.getBrokerIDFromFile(peerHostName, String.valueOf(peerPort), "files/brokerConfig.json");
                dataConnection = new Connection(peerHostName, peerPort, isAlive);
                dataConnMap.put(brokerCounter, dataConnection); // add connection to map, {5:conn5, 4:conn4, 3:conn3}
                try { // CHECK ACK
                    Thread.sleep(1000);
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
                System.out.println("sent peer info to broker " + peerID + "  " + peerHostName + ":" + peerPort + "...\n");

                brokerCounter++;  // next broker in the map

            }
        }
    }
}
