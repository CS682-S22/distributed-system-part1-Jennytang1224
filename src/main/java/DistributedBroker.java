import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.MessageInfo;
import dsd.pubsub.protos.PeerInfo;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class DistributedBroker {


    private String hostName;
    private int port;
    private static volatile boolean running = true;
    static Server server;
    private Connection connection;
    static String peerHostName;
    static int peerPort;
    static int messageCounter = 0;
    static int offsetInMem = 0;
    static List<HashMap<String,HashMap<Integer, CopyOnWriteArrayList<byte[]>>>> topicMapList = new ArrayList<>();
    static private HashMap<String, HashMap<Integer, CopyOnWriteArrayList<byte[]>>> topicMap;// <topic1: topic1_list, topic2: topic2_list>
    static int brokerID;

    public DistributedBroker(String hostName, int port) {
        this.hostName = hostName;
        this.port = port;
        this.topicMap = new HashMap<>();


    }


    // broker needs to constantly listen and
    // unpack proto buffer see if its producer or consumer connection, peerinfo
    /**
     * use threads to start the connections, receive and send data concurrently
     */
    public void run() throws IOException {

        Thread serverListener = new Thread(() -> {
            boolean running = true;
            try {
                this.server = new Server(this.port);
                System.out.println("A broker start listening on port: " + this.port + "...");
            } catch (IOException e) {
                e.printStackTrace();
            }
            while (running) {
                Connection connection = this.server.nextConnection(); // calls accept on server socket to block
                Thread serverReceiver = new Thread(new DistributedBroker.Receiver(this.hostName, this.port, connection));
                serverReceiver.start();
            }
        });
        serverListener.start(); // start listening ...

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

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
        int brokerID;


        public Receiver(String name, int port, Connection conn) {
            this.name = name;
            this.port = port;
            this.conn = conn;
            brokerID = Utilities.getBrokerIDFromFile(name, String.valueOf(port));
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

                if(counter == 0) { // first mesg is peerinfo
                    try {
                        p = PeerInfo.Peer.parseFrom(buffer);
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }

                    type = p.getType(); // consumer or producer
                    System.out.println("\n*** Broker " + port + ": New Connection coming in ***");
                    peerHostName = p.getHostName();
                    peerPort = p.getPortNumber();

                    if (type.equals("consumer")) {
                        System.out.println("this broker NOW has connected to consumer: " + peerHostName + " port: " + peerPort + "\n");
                        counter++;
//                    }else if (type.equals("producer")) {
//                        // get the messageInfo though socket
//                        System.out.println("this Broker now has connected to producer: " + peerHostName + " port: " + peerPort + "\n");
//                        counter++;
                    } else {
                        // get the messageInfo though socket
                        type = "producer"; // producer data send from load balancer directly, so no peerinfo
                        System.out.println(">> this Broker now has connected to producer ");
                        Thread th = new Thread(new ReceiveProducerData(buffer, topicMapList, messageCounter, offsetInMem, brokerID));
                        th.start();
                        try {
                            th.join();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        counter++;
                        messageCounter++;
                        //System.out.println("invalid type, should be either producer or consumer");
                        //System.exit(-1);
                    }

                }
                else{ // when receiving data
                    if (type.equals("producer")) {
                        Thread th = new Thread(new ReceiveProducerData(buffer, topicMapList, messageCounter, offsetInMem, brokerID));
                        th.start();
                        try {
                            th.join();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        counter++;
                        messageCounter++;
                    } else if (type.equals("consumer")) {
                        Thread th = new Thread(new SendConsumerData(conn, buffer, topicMapList, LoadBalancer.connMap, brokerID));
                        th.start();
                        try {
                            th.join();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        counter++;
                    } else {
                        System.out.println("invalid type, should be either producer or consumer");
                        // System.exit(-1);
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
////
////    //broker receive data from producer
////    public byte[] receive()  {
////        byte[] buffer = null;
////        try {
////            int length = input.readInt();
////            if(length > 0) {
////                buffer = new byte[length];
////                input.readFully(buffer, 0, buffer.length);
////            }
////        } catch (EOFException ignored) {} //No more content available to read
////        catch (IOException exception) {
////
////            System.err.printf(" Fail to receive message ");
////        }
////        return buffer;
////    }
//
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


    public synchronized void shutdown() {
        this.running = false;
    }


}
