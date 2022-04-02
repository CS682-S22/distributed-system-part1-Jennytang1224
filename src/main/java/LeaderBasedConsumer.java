import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.MessageInfo;
import dsd.pubsub.protos.PeerInfo;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LeaderBasedConsumer {
    private String brokerLocation;
    private String topic;
    private int startingPosition;
    private int brokerPort;
    private String brokerHostName;
    private Socket socket;
    private DataInputStream input;
    private DataOutputStream output;
    private Connection connection;
    private static String outputPath;
    static Server server;
    static int peerPort;
    static String peerHostName;
    static int receiverCounter = 0;
    Receiver newReceiver;
    public CS601BlockingQueue<MessageInfo.Message> bq;


    public LeaderBasedConsumer(String brokerLocation, String topic, int startingPosition) {
        this.brokerLocation = brokerLocation;
        this.topic = topic;
        this.startingPosition = startingPosition;
        this.brokerHostName = brokerLocation.split(":")[0];
        this.brokerPort = Integer.parseInt(brokerLocation.split(":")[1]);
        this.socket = null;
        this.bq = new CS601BlockingQueue<>(100);


        try {
            this.socket = new Socket(this.brokerHostName, this.brokerPort);
            this.connection = new Connection(this.socket);
            this.input = new DataInputStream(new BufferedInputStream(this.socket.getInputStream()));
            this.output = new DataOutputStream(new BufferedOutputStream(this.socket.getOutputStream()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("this consumer is connecting to broker " + brokerLocation);
        // draft peerinfo
        String type = "consumer";
        List<Object> maps = Utilities.readConfig();
        IPMap ipMap = (IPMap) maps.get(0);
        PortMap portMap = (PortMap) maps.get(1);
        String peerHostName = Utilities.getHostName();
        int peerPort = Integer.parseInt(portMap.getPortById(ipMap.getIdByIP(peerHostName)));

        PeerInfo.Peer peerInfo = PeerInfo.Peer.newBuilder()
                .setType(type)
                .setHostName(peerHostName)
                .setPortNumber(peerPort)
                .build();
        this.connection.send(peerInfo.toByteArray());
        //save consumer info to filename
        outputPath = "files/" + type + "_" + peerHostName + "_" + peerPort + "_output";
        System.out.println("consumer sends first msg to broker with its identity...\n");
        newReceiver = new Receiver(peerHostName, peerPort, this.connection,  this.bq, this.topic, this.startingPosition);
        Thread serverReceiver = new Thread(newReceiver);
        serverReceiver.start();

    }

    public byte[] poll(int timeout){
        CS601BlockingQueue<MessageInfo.Message> bq = newReceiver.getBq();
        byte[] m = bq.poll(timeout).toByteArray();
        return m;
    }

    public String getOutputPath(){
        return outputPath;
    }


    // send request to broker
    public void subscribe(String topic, int startingPosition){
        System.out.println("... Requesting topic: " + topic + " starting at position: " + startingPosition + "...");
        MessageInfo.Message request = MessageInfo.Message.newBuilder()
                .setTopic(topic)
                .setOffset(startingPosition)
                .build();
        this.connection.send(request.toByteArray());
    }

    public int getPositionCounter(){
        return newReceiver.getPositionCounter();
    }

    public CS601BlockingQueue<MessageInfo.Message> getBq(){
        return newReceiver.getBq();
    }


    /**
     * inner class Receiver
     */
    static class Receiver implements Runnable {
        private String name;
        private int port;
        private Connection conn;
        boolean receiving = true;
        private ExecutorService executor;
        int positionCounter;
        CS601BlockingQueue<MessageInfo.Message> bq;
        int startingPosition;
        String topic;

        public Receiver(String name, int port, Connection conn, CS601BlockingQueue<MessageInfo.Message> bq, String topic, int startingPosition) {
            this.name = name;
            this.port = port;
            this.conn = conn;
            this.executor = Executors.newSingleThreadExecutor();
            this.positionCounter = 0;
            this.bq = bq;
            this.startingPosition = startingPosition;
            this.topic = topic;
        }

        public int getPositionCounter(){
            return positionCounter;
        }

        public CS601BlockingQueue<MessageInfo.Message> getBq(){
            return this.bq;
        }


        @Override
        public void run() {
            MessageInfo.Message m = null;
            Runnable add = () -> {
                byte[] result = conn.receive();
                if (result != null) {
                    try {
                        this.bq.put(MessageInfo.Message.parseFrom(result));
                        positionCounter++;
                        System.out.println("Consumer added a record to the blocking queue...");
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }
                }
                else{
//                    System.out.println("received result is null");
                }
            };

            while(receiving) {
                executor.execute(add);
            }
        }
    }
}