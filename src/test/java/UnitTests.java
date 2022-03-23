import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.MessageInfo;
import dsd.pubsub.protos.PeerInfo;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**  run 1 broker + LB **/
class UnitTests {
    private String brokerLocation;
    private int brokerPort = 1420;
    private String brokerHostName = "Jennys-MacBook-Pro.local";
    private Socket socket;
    private DataInputStream input;
    private DataOutputStream output;
    private Connection LBConnection;
    private Connection brokerConnection;
    private Server server;

    PeerInfo.Peer p = null;
    String topic;
    int startingPosition;
    private int LBPort;
    private String LBHostName;
    String brokerConfigFile = "files/brokerConfig.json";


    DistributedBroker broker = new DistributedBroker(brokerHostName, brokerPort, brokerConfigFile);


    @BeforeEach
    void setUp() throws IOException {

        try {
            broker.run();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // run LB
        System.out.println("run LB ...");
        try {
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        LBHostName = "Jennys-MacBook-Pro.local";
        LBPort =  1431;
        try {
            this.socket = new Socket(this.LBHostName, this.LBPort);
            this.LBConnection = new Connection(this.socket);
            this.input = new DataInputStream(new BufferedInputStream(this.socket.getInputStream()));
            this.output = new DataOutputStream(new BufferedOutputStream(this.socket.getOutputStream()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        LBConnection = new Connection(socket);

        topic = "product";
        startingPosition = 2;
        brokerLocation = brokerHostName + ":" + brokerPort;
    }

    @Test
    public void test_getHostname() {
        assertEquals(Utilities.getHostName(), "Jennys-MacBook-Pro.local");
    }

    //check args:
    @Test
    public void test_validateArgsBroker() {
        assertFalse(Utilities.validateArgsBroker(new String[]{}));
        assertTrue(Utilities.validateArgsBroker(new String[]{"broker.json"}));
    }

    @Test
    public void test_validateArgsProducer() {
        assertFalse(Utilities.validateArgsProducer(new String[]{}));
        assertFalse(Utilities.validateArgsProducer(new String[]{"localhost:1222"}));
        assertTrue(Utilities.validateArgsProducer(new String[]{"localhost:1222", "test.txt"}));
    }

    @Test
    public void test_validateArgsLoadBalancer() {
        assertFalse(Utilities.validateArgsLoadBalancer(new String[]{}));
        assertFalse(Utilities.validateArgsLoadBalancer(new String[]{"broker.json"}));
        assertFalse(Utilities.validateArgsLoadBalancer(new String[]{"3", "5"}));
        assertTrue(Utilities.validateArgsLoadBalancer(new String[]{"3", "5", "broker.json"}));
    }


    @Test
    public void test_producer_send() {
        String type = "producer";
        String peerHostName = "Jennys-MacBook-Pro.local";
        int peerPort = 1400;

        PeerInfo.Peer peerInfo = PeerInfo.Peer.newBuilder()
                .setType(type)
                .setHostName(peerHostName)
                .setPortNumber(peerPort)
                .build();
        boolean success = LBConnection.send(peerInfo.toByteArray());
        assertTrue(success);

        MessageInfo.Message record = MessageInfo.Message.newBuilder()
                .setTopic(topic)
                .setKey("1")
                .setValue(ByteString.copyFromUtf8("data"))
                .setPartition(1)
                .setOffset(1)
                .build();
        // producer send record to broker
        LBConnection.send(record.toByteArray());

        MessageInfo.Message record2 = MessageInfo.Message.newBuilder()
                .setTopic(topic)
                .setKey("2")
                .setValue(ByteString.copyFromUtf8("data"))
                .setPartition(1)
                .setOffset(2)
                .build();
        // producer send record to broker
        LBConnection.send(record2.toByteArray());
    }

    @Test
    public void test_broker(){
//        try {
//            server = new Server(brokerPort);
//            //  System.out.println("A broker start listening on port: " + this.port + "...");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        Connection connection = this.server.nextConnection(); // calls accept on server socket to block
//        //Connection connection = new Connection(brokerHostName, brokerPort);
//        //  DistributedBroker.Receiver = new ReceiveProducerData(buffer, topicMapList, brokerID)
//        byte[] buffer = connection.receive();
//        System.out.println(buffer);
        assertEquals(broker.receiveMessageCounter(), 0);

    }

//    @Test
//    public void test_loadBalancer_receive() {
//        try {
//            this.socket = new Socket(brokerHostName, brokerPort);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        brokerConnection = new Connection(this.socket);
//        byte[] buffer = LBConnection.receive();
//        MessageInfo.Message d = null;
//        System.out.println(buffer);
//        try {
//            d = MessageInfo.Message.parseFrom(buffer);
//        } catch (InvalidProtocolBufferException e) {
//            e.printStackTrace();
//        }
//        assertEquals(d.getTopic(), topic);
//        assertEquals(d.getKey(), "123");
//        assertEquals(d.getOffset(), 1);
//    }

    @Test
    public void test_consumer_subscribe(){
        Consumer consumer = new Consumer(brokerLocation, topic, startingPosition);
        consumer.subscribe(topic, startingPosition);
        try { // every 3 sec request new data
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertEquals(0, consumer.getReceiverCounter());
    }


}