import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.PeerInfo;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class LoadBalancer {
    private String hostName;
    private int port;
    private Socket socket;
    private DataInputStream input;
    private DataOutputStream output;
    private static volatile boolean running = true;
    static HashMap<String, HashMap<Integer, CopyOnWriteArrayList<byte[]>>> topicMap;
    static Server server;
    private Connection connection;
    static String peerHostName;
    static int peerPort;
    static int messageCounter = 0;
    static int offsetInMem = 0;
    int numOfBrokers;
    int numOfPartitions;
    int brokerCounter = 1;
    static HashMap<Integer, Connection> connMap = new HashMap<>();
    static HashMap<String, Integer> counterMap = new HashMap<>();
    String brokerConfigFile;


    public LoadBalancer(String hostName, int port, int numOfBrokers, int numOfPartitions, String brokerConfigFile) {
        this.hostName = hostName;
        this.port = port;
        this.topicMap = new HashMap<>();
        this.numOfBrokers = numOfBrokers;
        this.numOfPartitions = numOfPartitions;
        this.brokerConfigFile = brokerConfigFile;

    }


    // broker needs to constantly listen and
    // unpack proto buffer see if its producer or consumer connection, peerinfo
    /**
     * use threads to start the connections, receive and send data concurrently
     */
    public void run() throws IOException {
        //start brokers:
        while (brokerCounter <= numOfBrokers) {

            List<Object> maps = Utilities.readBrokerConfig(brokerConfigFile);
            IPMap ipMap = (IPMap) maps.get(0);
            PortMap portMap = (PortMap) maps.get(1);
            String brokerHostName = ipMap.getIpById(String.valueOf(brokerCounter));
            int brokerPort =  Integer.parseInt(portMap.getPortById(String.valueOf(brokerCounter)));
            // create a connection to the broker
            try {
                this.socket = new Socket(brokerHostName, brokerPort);
                connection = new Connection(this.socket);
                connMap.put(brokerCounter, connection);
                System.out.println("Connected to broker: " + brokerHostName + ":" + brokerPort);
                this.input = new DataInputStream(new BufferedInputStream(this.socket.getInputStream()));
                this.output = new DataOutputStream(new BufferedOutputStream(this.socket.getOutputStream()));
            } catch (IOException e) {
                e.printStackTrace();
            }
            brokerCounter++;
        }

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
                Thread serverReceiver = new Thread(new LoadBalancer.Receiver(this.hostName, this.port, connection, this.numOfBrokers, this.numOfPartitions));
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
        int numOfBrokers;
        int numOfPartitions;

        public Receiver(String name, int port, Connection conn, int numberOfBrokers, int numOfPartitions) {
            this.name = name;
            this.port = port;
            this.conn = conn;
            this.numOfBrokers = numberOfBrokers;
            this.numOfPartitions = numOfPartitions;
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
                        } else {
                            System.out.println("invalid type, should be either producer or consumer");
                        }

                    }
                    else{ // when receiving data
                        if(type.equals("producer")) {
                            Thread th = new Thread(new ReceiveProducerMessage(buffer, messageCounter,
                                    offsetInMem, numOfBrokers, numOfPartitions, connMap, counterMap));
                            th.start();
                            try {
                                th.join();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            counter++;
                            messageCounter++;
                        }
                        else{
                            System.out.println("invalid type, should be either producer or consumer");
                        }
                    }
                }
            }
        }
    }
}

