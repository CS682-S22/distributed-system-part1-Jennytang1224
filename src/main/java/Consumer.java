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
        this.brokerHostName =brokerLocation.split(":")[0];
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
        peerHostName = "Jennys-MacBook-Pro.local";
        peerPort = 1435;
        PeerInfo.Peer peerInfo = PeerInfo.Peer.newBuilder()
                .setType(type)
                .setHostName(peerHostName)
                .setPortNumber(peerPort)
                .build();
        this.connection.send(peerInfo.toByteArray());
        //save consumer info to filename
        outputPath = type + "_" + peerHostName + "_" + peerPort + "_output";
        System.out.println("consumer sends first msg to broker with its identity...\n");

    }

    /**
     * use threads to start the connections, receive and send data concurrently
     */
    public void run() throws IOException{
        Thread serverListener = new Thread(() -> {
            boolean running = true;
            try {
                this.server = new Server(peerPort);
                System.out.println("this consumer starts listening on port: " + peerPort + "...");
            } catch (IOException e) {
                e.printStackTrace();
            }

            while (running) {
                System.out.println("while running...");
                Connection connection = this.server.nextConnection(); // calls accept on server socket to block
                System.out.println("new receiver thread...");
                Thread serverReceiver = new Thread(new Receiver(peerHostName, peerPort, connection));

                System.out.println("receiver starting...");
                serverReceiver.start();

            }
        });
        serverListener.start();
        System.out.println("listener starting...");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

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
            while (receiving) {
//                byte[] buffer = conn.receive();
//                try {
//                    Thread.sleep(100);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//                if (buffer == null || buffer.length == 0) {
//                    //  System.out.println("nothing received/ finished receiving");
//                }
//                try {
//                    m = MessageInfo.Message.parseFrom(buffer);
//                } catch (InvalidProtocolBufferException e) {
//                    e.printStackTrace();
//                }

//                String value = m.getValue(); // data
                System.out.println("add to bq:");
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
                        System.out.println("received result is null");
                    }
                };
                executor.execute(add);
                m = bq.poll(30);
                if (m != null) { // received within timeout
                    //save to file
                    byte[] arr = m.getValue().getBytes(StandardCharsets.UTF_8);
                    try {
                        writeBytesToFile(outputPath, arr);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

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
