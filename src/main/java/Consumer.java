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

public class Consumer {
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


    public Consumer(String brokerLocation, String topic, int startingPosition) {
        this.brokerLocation = brokerLocation;
        this.topic = topic;
        this.startingPosition = startingPosition;
        this.brokerHostName = brokerLocation.split(":")[0];
        this.brokerPort = Integer.parseInt(brokerLocation.split(":")[1]);
        this.socket = null;

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
        outputPath = type + "_" + peerHostName + "_" + peerPort + "_output";
        System.out.println("consumer sends first msg to broker with its identity...\n");

        Thread serverReceiver = new Thread(new Receiver(peerHostName, peerPort, this.connection));
        serverReceiver.start();
    }

    public byte[] poll(Duration timeout){
        return new byte[0];
    }

    // send request to broker
    public void subscribe(String topic, int startingPosition){
        MessageInfo.Message request = MessageInfo.Message.newBuilder()
                .setTopic(topic)
                .setOffset(startingPosition)
                .build();
        writeToSocket(request.toByteArray());
    }


    /**
     * inner class Receiver
     */
    static class Receiver implements Runnable {
        private String name;
        private int port;
        private Connection conn;
        boolean receiving = true;
        private CS601BlockingQueue<MessageInfo.Message> bq;
        private ExecutorService executor;

        public Receiver(String name, int port, Connection conn) {
            this.name = name;
            this.port = port;
            this.conn = conn;
            this.bq = new CS601BlockingQueue<>(3);
            this.executor = Executors.newSingleThreadExecutor();
        }

        @Override
        public void run() {
            MessageInfo.Message m = null;
            Runnable add = () -> {
                byte[] result = conn.receive();
                if (result != null) {
                    try {
                        bq.put(MessageInfo.Message.parseFrom(result));
                        System.out.println("a record has been put into the bq...");
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }
                }
                else{
//                    System.out.println("received result is null");
                }
            };

            while (receiving) {
                executor.execute(add);
                m = bq.poll(30);
                if (m != null) { // received within timeout
                    //save to file
                    byte[] arr = m.getValue().getBytes(StandardCharsets.UTF_8);
                    try {
                        writeBytesToFile("files/" + outputPath, arr);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                else{
//                    System.out.println("m == null");
                }
            }
        }
    }

    /**
     * write bytes to files
     */
    private static void writeBytesToFile(String fileOutput, byte[] buf)
            throws IOException {
        try (FileOutputStream fos = new FileOutputStream(fileOutput, true)) {
            System.out.println("writing to the file...");
            fos.write(buf);
            fos.write(10);
            fos.flush();
        }
        catch(IOException e){
            System.out.println("file writing error :(");
        }
    }

    public void writeToSocket(byte[] message){
        try {
            this.output.writeInt(message.length);
            this.output.write(message);
            this.output.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void close(){

    }
}
