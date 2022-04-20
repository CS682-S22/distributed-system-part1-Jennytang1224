

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.*;

import javax.xml.crypto.Data;
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
import java.util.concurrent.atomic.AtomicInteger;


public class LeaderBasedBroker {
    private static String hostName;
    private static int port;
    private ServerSocket serverSocket;
    private Socket socket;
    private DataInputStream input;
    private DataOutputStream output;
    private static volatile boolean running = true;
    static CopyOnWriteArrayList<ByteString> topicList;
    static Map<String, CopyOnWriteArrayList<ByteString>> topicMap;// <topic1: topic1_list, topic2: topic2_list>
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
    static int dataPort;
    static Server dataServer;
    static HashMap<Integer, Connection> dataConnMap = new HashMap<>();
    static int dataCounter = 1;
    static boolean synchronous;
    static Connection connWithProducer;
    static Connection connWithConsumer;
    static DataReceiver dr;
    static SynchronousReplication synchronousReplication;
    static AsynchronousReplication asynchronousReplication;
    static Connection dataConnection;
    static boolean isFailure;


    public LeaderBasedBroker(String hostName, int port, int dataPort, boolean synchronous, boolean isFailure) {
        this.hostName = hostName;
        this.port = port;
        this.topicList = new CopyOnWriteArrayList<>();
        this.topicMap = new HashMap<>();
        this.executor = Executors.newSingleThreadExecutor();
        this.dataPort = dataPort;
        this.synchronous = synchronous;
        this.isFailure = isFailure;
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
                int clearCounter = 0;
                dataConnection = this.dataServer.nextConnection(); // calls accept on server socket to block
                dr = new DataReceiver(this.hostName, this.dataPort, dataConnection, dataConnMap,
                        synchronous, dataCounter, topicMap, membershipTable, clearCounter);
                Thread dataServerReceiver = new Thread(dr);
                dataServerReceiver.start();
            }
        });
        dataServerListener.start();

        try {
            Thread.sleep(5000);
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
        int messageCounter = 0;
        int replicaCounter = 0;


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
                            peerID = Utilities.getBrokerIDFromFile(peerHostName, String.valueOf(peerPort), "files/brokerConfig.json");
                            int peerDataPort = Utilities.getPortRepByID(peerID);
                            int peerPort = Utilities.getPortByID(peerID);
                            if(membershipTable.membershipTable.containsKey(peerID) && !membershipTable.getMemberInfo(peerID).isAlive) {//new broker join
                                System.out.println("%%%%%%%%%%%%%%%%%%%%%%%% new data connection join");
                                dataConnection = new Connection(peerHostName, peerDataPort, true);
                                Connection regularConnection = new Connection(peerHostName, peerPort, true);
                                System.out.println("made data connection with peer: " + peerHostName + ":" + peerDataPort);
                                System.out.println("made connection with peer: " + peerHostName + ":" + peerPort);

                                //  peerID = Utilities.getBrokerIDFromFile(peerHostName, String.valueOf(peerPort), "files/brokerConfig.json");
                                dataConnMap.put(peerID, dataConnection); // add connection to map, {5:conn5, 4:conn4, 3:conn3}
                                connMap.put(peerID, regularConnection);

                                try {
                                    Thread.sleep(2000);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                                //  send peer info to other brokers
                                String type = "broker";
                                PeerInfo.Peer peerInfo = PeerInfo.Peer.newBuilder()
                                        .setType(type)
                                        .setHostName(hostName)
                                        .setPortNumber(dataPort)
                                        .build();

                                dataConnection.send(peerInfo.toByteArray());
                                regularConnection.send(peerInfo.toByteArray());
                               // System.out.println("sent peer info to broker " + peerID + "  " + peerHostName + ":" + peerPort + "...");

                            }

                        }
                        else if (type.equals("consumer")) { // hear from producer/LB only bc this broker is a leader
                            System.out.println("this Broker now has connected to CONSUMER: " + peerHostName + " port: " + peerPort + "\n");
                            counter++;
                            connWithConsumer = conn;
                        }
                    }

                    else {
                        if (type.equals("producer")) {  // when receiving data from producer

                            System.out.println("!!!!!!!!! receiving data from producer...");
                           // synchronized (this) {
                                //save data to my topic map:
                                Thread th = new Thread(new LeaderBasedReceiveProducerData(conn, ByteString.copyFrom(buffer), topicMap, messageCounter++, false));
                                th.start();
                                try {
                                    th.join();
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
//                                messageCounter++;
//                                System.out.println(">>> data stored and number of data received: " + messageCounter);

                                //replications:
                                if(!synchronous) { //replication with Asynchronous followers
                                    asynchronousReplication = new AsynchronousReplication(membershipTable, buffer, brokerID, dataConnMap, -1, topicMap, conn, false);//use data connections
                                 //   replication.run();
                                    Thread rep = new Thread(asynchronousReplication);
                                    rep.start();
                                    //send producer a big ack for next data
                                    System.out.println("sending big ack to producer WITHOUT CONFIRMING all followers get the data");
                                    Acknowledgment.ack ackToProducer = Acknowledgment.ack.newBuilder()
                                            .setSenderType("leadBrokerACK")
                                            .build();
                                    connWithProducer.send(ackToProducer.toByteArray());
                                }else{ //replication with Synchronous followers
                                    //need to send ack
                                    synchronousReplication = new SynchronousReplication(membershipTable, buffer, brokerID, dataConnMap, isFailure, messageCounter);//use data connections
                                    //   replication.run();
                                    Thread rep = new Thread(synchronousReplication);
                                    rep.start();
                                    try { // to get tha ack count
                                        Thread.sleep(1900);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }

                                    for (int id : membershipTable.getKeys()) {
                                        if (membershipTable.getMemberInfo(id).isAlive && id != brokerID) {
                                            synchronousReplication.numOfAckNeeded.getAndIncrement();
                                        }
                                    }
                                    System.out.println("RECEIVED ACK COUNT:" + DataReceiver.ackCount + " COLLECTING ACK COUNT: " + synchronousReplication.numOfAckNeeded);
                                    if(DataReceiver.ackCount.intValue() >= synchronousReplication.numOfAckNeeded.intValue()){//all followers get replica
                                        //send producer a big ack for next data
                                        System.out.println("sending big ack to producer WITH CONFIRMING all followers get the data");
                                        Acknowledgment.ack ackToProducer = Acknowledgment.ack.newBuilder()
                                                .setSenderType("leadBrokerACK")
                                                .build();
                                        connWithProducer.send(ackToProducer.toByteArray());
                                    }else{
                                        Acknowledgment.ack ackToProducer = Acknowledgment.ack.newBuilder()
                                                .setSenderType("leadBrokerNOACK")
                                                .build();
                                        connWithProducer.send(ackToProducer.toByteArray());
                                    }
                                    DataReceiver.ackCount.getAndSet(0); //important!! reset to 0
                                }
                           // }
                            counter++;
                           // messageCounter++;
                        }

                        else if (type.equals("consumer")){
                            System.out.println("receiving consumer request...");
                            Thread th = new Thread(new LeaderBasedSendConsumerData(connWithConsumer, buffer, topicMap));
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
                                   // System.out.println("receiving heartbeat from broker " + senderId + "...");

                                    // receive heartbeat from broker and response with its own id
                                    Resp.Response heartBeatResponse = Resp.Response.newBuilder()
                                            .setType("heartbeat").setSenderID(brokerID).build();
                                    conn.send(heartBeatResponse.toByteArray());
                                    try {
                                        Thread.sleep(300);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                    System.out.println("(received and replying heart beat to: " + senderId + ")");
                                 //   if(!membershipTable.getMemberInfo(senderId).isAlive) {
                                       membershipTable.getMemberInfo(senderId).setAlive(true);
                                        membershipTable.print();
                                        Utilities.sendMembershipTableUpdates(connMap.get(0), "updateAlive", brokerID, peerID,
                                                "", 0, "", membershipTable.getMemberInfo(peerID).isLeader, true);
                                 //   }


                                } else if (inElection && !f.getType().equals("heartbeat")) { // if election msg
                                    int senderId = f.getSenderID();
                                    int newLeader = f.getWinnerID();
                                    System.out.println(" -> > > receiving election msg from peer " + senderId);
                                    if (newLeader != -1) {//if other inform me new leader, me updated table
                                        System.out.println("        *** NEW LEADER ID:" + newLeader + " ***");

                                        int oldLeader = membershipTable.getLeaderID();
                                        if (oldLeader != -1) { // there's a leader
                                            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~ in my table updated the new leader to " + newLeader);
                                            membershipTable.switchLeaderShip(oldLeader, newLeader);//update new leader
                                        } else {
                                            System.out.println("weird ... no current leader right now");
                                        }
                                        membershipTable.getMemberInfo(senderId).setAlive(true);
                                        membershipTable.print();

                                        //see if I as new leader has the newest data by comparing topicMap size
                                        int currentMapSize = 0;
                                        for(Map.Entry<String, CopyOnWriteArrayList<ByteString>> entry : topicMap.entrySet()) {
                                            for(int i = 0; i < entry.getValue().size(); i++){
                                                currentMapSize++;
                                            }
                                        }

                                     //   System.out.println("topic map size before sending to compare: " + currentMapSize);
                                        for (int id : membershipTable.getKeys()) {
                                            if(membershipTable.getMemberInfo(id).isAlive && id != brokerID) {
                                                System.out.println("sending request to broker " + id + " to compare data");
                                                Acknowledgment.ack requestNewestData = Acknowledgment.ack.newBuilder()
                                                        .setSenderType("requestNewestData")
                                                        .setNum(currentMapSize) // my topicMap size
                                                        .setLeadBrokerLocation(String.valueOf(brokerID))
                                                        .build();
                                                dataConnMap.get(id).send(requestNewestData.toByteArray());
                                            }
                                        }



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


                                        Resp.Response heartBeatMessage = Resp.Response.newBuilder().setType("heartbeat").setSenderID(brokerID).build();
                                        conn.send(heartBeatMessage.toByteArray());
                                        inElection = false;

                                    } else { //other sends election msg to me, me needs reply to other broker
                                        Resp.Response electionResponse = Resp.Response.newBuilder().setType("election")
                                                .setSenderID(brokerID).setWinnerID(-1).build();
                                        conn.send(electionResponse.toByteArray());

                                        System.out.println(" -> > > broker " + brokerID + " reply to broker " + senderId + " election msg...");
                                        inElection = true;
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
                //boolean alive = m.isAlive;
              //  Connection con = new Connection(peerHost, peerPortNum, alive); // make connection to peers in config
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



    /**
     * inner class Connector for all brokers on replication
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
              //  if (!dataConnMap.containsKey(brokerCounter)) {
                    String peerHostName = Utilities.getHostnameByID(brokerCounter);
                    int peerPort = Utilities.getPortRepByID(brokerCounter);
                    peerID = Utilities.getBrokerIDFromFile(peerHostName, String.valueOf(peerPort), "files/brokerConfig.json");
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
               // }
                }
            //    get all data from the leader:
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
}
