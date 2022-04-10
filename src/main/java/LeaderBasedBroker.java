

import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.HeartBeatMessage;
import dsd.pubsub.protos.PeerInfo;
import dsd.pubsub.protos.Resp;
import dsd.pubsub.protos.Response;

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
    //static int brokerID;
    private String brokerConfigFile = "files/brokerConfig.json";
    private static int numOfBrokers = 5;
    private int brokerCounter = 1;
    static HashMap<Integer, Connection> connMap = new HashMap<>();
    private static MembershipTable membershipTable = new MembershipTable();
    static volatile boolean inElection = false;


    public LeaderBasedBroker(String hostName, int port) {
        this.hostName = hostName;
        this.port = port;
        this.topicList = new CopyOnWriteArrayList<>();
        this.topicMap = new HashMap<>();
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

//
//        Connector c = new Connector(this.hostName, this.port);
//        c.run();

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
                   // System.out.println("nothing has received");
                }
                else {
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
                        peerID = Utilities.getBrokerIDFromFile(peerHostName, String.valueOf(peerPort), "files/brokerConfig.json");

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
                            // other send me heartbeat msg(me reply) / election(other inform new leader, me update table)/ election(other send election msg to me, needs reply to other broker )
                            Resp.Response f = null;
                            try {
                                f = Resp.Response.parseFrom(buffer);
                            } catch (InvalidProtocolBufferException exception) {
                                exception.printStackTrace();
                            }
                            if(f != null) {
                                System.out.println("type in broker: " + f.getType());
                                if (f.getType().equals("heartbeat")) {
//                                    if(inElection){
//                                        try {
//                                            Thread.sleep(4000);
//                                        } catch (InterruptedException e) {
//                                            e.printStackTrace();
//                                        }
//                                    }
                                    inElection = false;
                                } else if (f.getType().equals("election")) {
                                    inElection = true;
                                } else {
                                    System.out.println("wrong type");
                                }

                                System.out.println("election in receiver:" + inElection);
                                if (!inElection) { // if its heartbeat, me reply
                                    int senderId = f.getSenderID();
                                    System.out.println("broker " + brokerID + " receiving heartbeat from broker " + senderId + "...");
                                    // receive heartbeat from broker and response with its own id
                                    Resp.Response heartBeatResponse = Resp.Response.newBuilder()
                                            .setType("heartbeat").setSenderID(brokerID).build();
                                    conn.send(heartBeatResponse.toByteArray());
                                    try {
                                        Thread.sleep(2000);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                    membershipTable.getMemberInfo(senderId).setAlive(true);

                                }


                                else if(inElection && !f.getType().equals("heartbeat")){ // if election msg
                                    int senderId = f.getSenderID();
                                    int newLeader = f.getWinnerID();
                                    System.out.println(" -> > > receiving election msg from peer " + senderId);
                                    if (newLeader != -1) {//if other inform me new leader, me updated table
                                        System.out.println("new leader id:" + newLeader);
                                        int oldLeader = membershipTable.getLeaderID();
                                        System.out.println("old leader id:" + oldLeader);
                                        if (oldLeader != -1) { // there's a leader
                                            System.out.println("in my table updated the new leader to " + newLeader );
                                            membershipTable.switchLeaderShip(oldLeader, newLeader);//update new leader
                                        } else {
                                            System.out.println("weird ... no current leader right now");
                                        }
                                    //    inElection = false; // election ended on my end
                                        membershipTable.getMemberInfo(senderId).setAlive(true);
                                        membershipTable.print();
                                        System.out.println("!!!!!!!!! election ended on broker " + brokerID + " side!");
                                        inElection = false;


                                        Resp.Response heartBeatMessage = Resp.Response.newBuilder().setType("heartbeat").setSenderID(brokerID).build();
                                        conn.send(heartBeatMessage.toByteArray());
//                                        try {
//                                            Thread.sleep(1000);
//                                        } catch (InterruptedException e) {
//                                            e.printStackTrace();
//                                        }

                                    }
                                    else { //other sends election msg to me, me needs reply to other broker
                                        Resp.Response electionResponse = Resp.Response.newBuilder().setType("election")
                                                .setSenderID(brokerID).setWinnerID(-1).build();
                                        conn.send(electionResponse.toByteArray());
                                        inElection = true;
                                        System.out.println(" -> > >broker " + brokerID + " reply to broker " + senderId  + " election msg...");
                                    }
                                }
                            }
                            counter++;
                        }
                        else {
                        System.out.println("invalid type, should be either producer or consumer");
                        // System.exit(-1);
                        }
                    }
                }
            }
        }
    }


    /**
     * inner class Connector for all brokers
     */
    static class Connector implements Runnable {
        int brokerCounter = 1;
        private String brokerConfigFile = "files/brokerConfig.json";
        Connection connection;
        String hostName;
        int port;
        int brokerID;
        int peerID;
        boolean isAlive = true;


        public Connector(String hostName, int port) {
            this.hostName = hostName;
            this.port = port;
            this.brokerID = Utilities.getBrokerIDFromFile(hostName, String.valueOf(port), "files/brokerConfig.json");

        }

        @Override
        public void run() {
            // create connections to all other lower brokers than itself
            while (brokerCounter <= numOfBrokers) { // need to generalize
                List<Object> maps = Utilities.readBrokerConfig(brokerConfigFile);
                IPMap ipMap = (IPMap) maps.get(0);
                PortMap portMap = (PortMap) maps.get(1);
                String peerHostName = ipMap.getIpById(String.valueOf(brokerCounter));
                int peerPort = Integer.parseInt(portMap.getPortById(String.valueOf(brokerCounter)));
                peerID = Utilities.getBrokerIDFromFile(peerHostName, String.valueOf(peerPort), "files/brokerConfig.json");

                connection = new Connection(peerHostName, peerPort, isAlive); // connection to other peer

                //add this broker to membership table
                isAlive = connection.getAlive();
                boolean isLeader = false;
                if (peerID == 1) {//leader
                    isLeader = true;
                }
                if (connection == null) {
                    System.out.println("(This broker is NOT in use)");
                    return;
                }
                MemberInfo memberInfo = new MemberInfo(peerHostName, peerPort, "", isLeader, isAlive);

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

                connection.send(peerInfo.toByteArray());
                System.out.println("sent peer info to broker " + peerID + "...\n");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (isAlive) {
                    System.out.println("Now sending heartbeat to " + peerID + "...\n");
                    HeartBeatSender sender = new HeartBeatSender(this.hostName,
                            String.valueOf(this.port), connection, peerHostName, peerPort,
                            connMap, membershipTable, inElection);
                    Thread heartbeatSender = new Thread(sender);
                    heartbeatSender.start();

                }

                brokerCounter++;  // next broker in the map
            }
//            // send heartbeat to others
//            for(int id: membershipTable.getKeys()) {
//                isAlive = membershipTable.getMemberInfo(id).isAlive;
//                if (isAlive) {
//                    System.out.println("Now sending heartbeat to " + peerID + "...\n");
//                    HeartBeatSender sender = new HeartBeatSender(this.hostName,
//                            String.valueOf(this.port), connection, peerHostName, peerPort,
//                            connMap, membershipTable, inElection);
//                    Thread heartbeatSender = new Thread(sender);
//                    heartbeatSender.start();
//
//                }
//            }



        }

    }
}
