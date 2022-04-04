import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.PeerInfo;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class LeaderBasedLoadBalancer {
    private String hostName;
    private int port;
    private Socket socket;
    private DataInputStream input;
    private DataOutputStream output;
    private static volatile boolean running = true;
    static Map<String, CopyOnWriteArrayList> topicMap;// <topic1: topic1_list, topic2: topic2_list>
    private static Server server;
    private Connection connection;
    private static String peerHostName;
    private static int peerPort;
    private static int messageCounter = 0;
    private static int offsetInMem = 0;
    private int numOfBrokers;
    private int numOfPartitions;
    private int brokerCounter = 1;
    static HashMap<Integer, Connection> connMap = new HashMap<>();
    private static HashMap<String, Integer> counterMap = new HashMap<>();
    private String brokerConfigFile;


    public LeaderBasedLoadBalancer(String hostName, int port, int numOfBrokers, int numOfPartitions, String brokerConfigFile) {
        this.hostName = hostName;
        this.port = port;
        this.topicMap = new HashMap<>();
        this.numOfBrokers = numOfBrokers;
        this.numOfPartitions = numOfPartitions;
        this.brokerConfigFile = brokerConfigFile;
    }


    /**
     * load balancer create broker connections, and listening from producer
     */
    public void run() throws IOException {


        // start listening
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
//        int numOfBrokers;
//        int numOfPartitions;

        public Receiver(String name, int port, Connection conn) {
            this.name = name;
            this.port = port;
            this.conn = conn;
//            this.numOfBrokers = numberOfBrokers;
//            this.numOfPartitions = numOfPartitions;
        }

        @Override
        public void run() {
            PeerInfo.Peer p = null;
            while (receiving) {
                byte[] buffer = conn.receive();
                if (buffer == null || buffer.length == 0) {
                    // System.out.println("nothing received/ finished receiving");
                }
                else {
                    if(counter == 0) { // first mesg is peerinfo
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
                            // get the messageInfo though socket
                            System.out.println("Load Balancer now has connected to producer: " + peerHostName + " port: " + peerPort + "\n");
                            counter++;
                        }  else if (type.equals("consumer")) {
                            System.out.println("this broker NOW has connected to consumer: " + peerHostName + " port: " + peerPort + "\n");
                            counter++;
                        }


                    }
                    else{ // when receiving data
                        if(type.equals("producer")) {
                            Thread th = new Thread(new LeaderBasedReceiveProducerMessage(buffer, messageCounter, counterMap, conn));
                            th.start();
                            try {
                                th.join();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            counter++;
                            messageCounter++;
                        }
                        else if (type.equals("consumer")) {
                            Thread th = new Thread(new LeaderBasedSendConsumerData(conn, buffer, topicMap));
                            th.start();
                            try {
                                th.join();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            counter++;
                        }
                    }
                }
            }
        }
    }
}
