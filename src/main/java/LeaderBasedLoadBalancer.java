import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.Acknowledgment;
import dsd.pubsub.protos.BrokerToLoadBalancer;
import dsd.pubsub.protos.PeerInfo;
import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class LeaderBasedLoadBalancer {
    private String hostName;
    private int port;
    private static volatile boolean running = true;
    static Map<String, CopyOnWriteArrayList> topicMap;// <topic1: topic1_list, topic2: topic2_list>
    private static Server server;
    private static String peerHostName;
    private static int peerPort;
    private int numOfBrokers;
    private int numOfPartitions;
    private static HashMap<String, Integer> counterMap = new HashMap<>();
    private static MembershipTable membershipTable = new MembershipTable();
    static int currentLeadBroker = 1;
    static Connection connWithConsumer;



    public LeaderBasedLoadBalancer(String hostName, int port) {
        this.hostName = hostName;
        this.port = port;
        this.topicMap = new HashMap<>();
        this.numOfBrokers = numOfBrokers;
        this.numOfPartitions = numOfPartitions;
    }


    /**
     * load balancer create broker connections, and listening from producer
     */
    public void run() throws IOException {
        Thread serverListener = new Thread(() -> {
            boolean running = true;
            try {
                this.server = new Server(this.port);
                System.out.println("\nLoad balancer start listening on port: " + this.port + "...");
            } catch (IOException e) {
                e.printStackTrace();
            }
            while (running) {
                Connection connection = this.server.nextConnection(); // calls accept on server socket to block
                Thread serverReceiver = new Thread(new LeaderBasedLoadBalancer.Receiver(this.hostName, this.port, connection));
                serverReceiver.start();
            }
        });
        serverListener.start(); // start listening ...
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
        private int dataCounter = 0;

        public Receiver(String name, int port, Connection conn) {
            this.name = name;
            this.port = port;
            this.conn = conn;
        }

        public void logistic() { // inside receiver
            System.out.println("TABLE ON LB after receiving updated table from leader broker:");
            membershipTable.print();
            counter++;
            currentLeadBroker = membershipTable.getLeaderID();
            System.out.println("currentLeadBroker: " + currentLeadBroker);
        }

        @Override
        public void run() {
            PeerInfo.Peer p = null;
            while (receiving) {
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
                        System.out.println("\n *** New Connection coming in -> peer type: " + type + " ***");
                        peerHostName = p.getHostName();
                        peerPort = p.getPortNumber();

                        if (type.equals("producer")) {
                            System.out.println("Load Balancer NOW has connected to producer: " + peerHostName + " port: " + peerPort + "\n");
                            counter++;
                            //send lead broker location to producer
                            String leadBrokerName = membershipTable.getMemberInfo(currentLeadBroker).getHostName();
                            String leadBrokerPort = String.valueOf(membershipTable.getMemberInfo(currentLeadBroker).getPort());
                            String leadBrokerLocation = leadBrokerName + ":" + leadBrokerPort;
                            Acknowledgment.ack response = Acknowledgment.ack.newBuilder()
                                    .setSenderType("loadBalancer")
                                    .setLeadBrokerLocation(leadBrokerLocation)
                                    .build();
                            conn.send(response.toByteArray());
                            System.out.println("LB sends leadBrokerLocation to producer: " + leadBrokerLocation);
                        } else if (type.equals("consumer")) {
                            System.out.println("Load Balancer NOW has connected to consumer: " + peerHostName + " port: " + peerPort + "\n");
                            connWithConsumer = conn;//save the connection with consumer for sending data back later
                            counter++;
                            //send consumer broker location
                            String leadBrokerName = membershipTable.getMemberInfo(currentLeadBroker).getHostName();
                            String leadBrokerPort = String.valueOf(membershipTable.getMemberInfo(currentLeadBroker).getPort());
                            String leadBrokerLocation = leadBrokerName + ":" + leadBrokerPort;
                            Acknowledgment.ack response = Acknowledgment.ack.newBuilder()
                                    .setSenderType("loadBalancer")
                                    .setLeadBrokerLocation(leadBrokerLocation)
                                    .build();
                            conn.send(response.toByteArray());
                            System.out.println("LB sends leadBrokerLocation to consumer: " + leadBrokerLocation);

                        } else if (type.equals("broker")) {
                            System.out.println("Load Balancer NOW has connected to load balancer: " + peerHostName + " port: " + peerPort + "\n");
                            counter++;
                        }

                    } else { // when receiving data
                        if (type.equals("broker")) { //load Balancer hear from leader broker
                            // update membership table
                            BrokerToLoadBalancer.lb b = null;
                            try {
                                b = BrokerToLoadBalancer.lb.parseFrom(buffer);
                            } catch (InvalidProtocolBufferException e) {
                                e.printStackTrace();
                            }
                            if (b != null) {
                                String type = b.getType();
                                int peerID = b.getBrokerID();

                                if (type.equals("new")) {
                                    MemberInfo m = new MemberInfo(b.getHostName(), b.getPort(),
                                            b.getToken(), b.getIsLeader(), b.getIsAlive());
                                    membershipTable.put(peerID, m);
                                    logistic();
                                } else if (type.equals("updateAlive")) {
                                    membershipTable.getMemberInfo(peerID).setAlive(b.getIsAlive());
                                    logistic();
                                } else if (type.equals("updateLeader")) {
                                    membershipTable.getMemberInfo(peerID).setAlive(b.getIsAlive());
                                    membershipTable.getMemberInfo(peerID).setLeader(b.getIsLeader());
                                    logistic();
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
