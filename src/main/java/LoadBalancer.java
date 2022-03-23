import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.MessageInfo;
import dsd.pubsub.protos.PeerInfo;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class LoadBalancer {
    private String hostName;
    private int port;
    private ServerSocket serverSocket;
    private Socket socket;
    private DataInputStream input;
    private DataOutputStream output;
    private static volatile boolean running = true;
   // static CopyOnWriteArrayList<byte[]> topicList;
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
//        this.topicList = new CopyOnWriteArrayList<>();
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
//            DistributedBroker broker = new DistributedBroker(brokerHostName, brokerPort);
//            try {
//                broker.run();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
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


        //receive peer info

//        Thread clientSender = new Thread(new Sender(this.hostName, String.valueOf(this.port), this.connection));
//        clientSender.start();
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
                //   System.out.println("broker receiving data from connection ");
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
//                        } else if (type.equals("consumer")) {
//                            System.out.println("this broker NOW has connected to consumer: " + peerHostName + " port: " + peerPort + "\n");
//                            counter++;
                        } else {
                            System.out.println("invalid type, should be either producer or consumer");
                            //System.exit(-1);
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
//                        else if(type.equals("consumer")){
////                            Thread th = new Thread(new SendConsumerData(conn, buffer, topicMap, connMap));
////                            th.start();
////                            try {
////                                th.join();
////                            } catch (InterruptedException e) {
////                                e.printStackTrace();
////                            }
////                            counter++;
//
//                            conn.send(String.valueOf("number of brokers:" + numOfBrokers).getBytes(StandardCharsets.UTF_8));
//                        }
                        else{
                            System.out.println("invalid type, should be either producer or consumer");
                            // System.exit(-1);
                        }
                    }
                }
            }
        }
    }



//    /**
//     * write bytes to files
//     */
//    private static void writeBytesToFile(String fileOutput, byte[] buf)
//            throws IOException {
//        try (FileOutputStream fos = new FileOutputStream(fileOutput, true)) {
//            System.out.println("writing to the file...");
//            fos.write(buf);
//        }
//        catch(IOException e){
//            System.out.println("file writing error :(");
//        }
//
//    }

//    //broker receive data from producer
//    public byte[] receive()  {
//        byte[] buffer = null;
//        try {
//            int length = input.readInt();
//            if(length > 0) {
//                buffer = new byte[length];
//                input.readFully(buffer, 0, buffer.length);
//            }
//        } catch (EOFException ignored) {} //No more content available to read
//        catch (IOException exception) {
//
//            System.err.printf(" Fail to receive message ");
//        }
//        return buffer;
//    }

//    // write received record in bytes to the list
//    public static void writeToCluster(byte[] recordBytes){
//        MessageInfo.Message d = null;
//        try {
//            d = MessageInfo.Message.parseFrom(recordBytes);
//        } catch (InvalidProtocolBufferException e) {
//            e.printStackTrace();
//        }
//        String topic = d.getTopic();
//        //   if(running) {
//        if(topicMap.containsKey(topic)){ //if key is in map
//            topicMap.get(topic).add(recordBytes);
//        }
//        else{ //if key is not in the map, create CopyOnWriteArrayList and add first record
//            CopyOnWriteArrayList newList = new CopyOnWriteArrayList<>();
//            newList.add(recordBytes);
//            topicMap.put(topic, newList);
//        }
//        //   }
//        //  System.out.println("topic map size: " + topicMap.size());
//
//    }
//
//    // read from broker 1 with input topics
//    public static void readFromCluster(byte[] recordBytes, Connection conn){
//        // if(running) {
//
//        MessageInfo.Message d = null;
//        try {
//            d = MessageInfo.Message.parseFrom(recordBytes);
//        } catch (InvalidProtocolBufferException e) {
//            e.printStackTrace();
//        }
//        String topic = d.getTopic();
//        System.out.println("consumer subscribed to: " + topic);
//        int startingPosition = d.getOffset();
//
//        //in the hashmap, get the corresponding list of this topic
//        if (!topicMap.containsKey(topic)) {
//            System.out.println("No topic called '" + topic + "' in the broker!");
//        }
//        CopyOnWriteArrayList<byte[]> topicList = topicMap.get(topic);
//
//        // start getting the all record from this topic
//        for (int i = startingPosition; i < topicList.size(); i++) {
//            byte[] singleRecord = topicList.get(i);
//            // send ALL record in this list to the consumer
//            conn.send(singleRecord);
//            System.out.println("A record has sent to the consumer: " + peerHostName + ":" + peerPort);
//        }
//
//
//        //notify other brokers to send their data related to this topic to the consumer
////            notifyOtherBrokers();
//        //  }
//    }
//
//
//    public synchronized void shutdown() {
//        this.running = false;
//    }
}

