import Project2.Consumer;
import Project2.DistributedBroker;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.MessageInfo;
import dsd.pubsub.protos.PeerInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.lang.management.MemoryManagerMXBean;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**  run 1 broker + LB **/
class UnitTests {
    private String brokerLocation;
    private int brokerPort = 1131;
    private int brokerDataPort = 1141;
    private String brokerHostName = "Jennys-MacBook-Pro.local";
    private Socket socket;
    private DataInputStream input;
    private DataOutputStream output;
    private Connection LBConnection;
    private Connection brokerConnection;
    private Server server;
    String LBLocation;

    PeerInfo.Peer p = null;
    String topic;
    int startingPosition;
    private int LBPort;
    private String LBHostName;

    LeaderBasedLoadBalancer lb = new LeaderBasedLoadBalancer(brokerHostName, 1130);
    LeaderBasedBroker broker1 = new LeaderBasedBroker(brokerHostName, 1131, 1141, true, false);
    LeaderBasedBroker broker2 = new LeaderBasedBroker(brokerHostName, 1132, 1142, true, false);

    @BeforeEach
    void setUp() throws IOException {
        try { // run LB first
            lb.run();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try { // next run broker
            broker1.run();
        } catch (IOException e) {
            e.printStackTrace();
        }

        LBLocation = LBHostName + ":" + LBPort;
        topic = "product";
        startingPosition = 2;
        brokerLocation = brokerHostName + ":" + brokerPort;

    }


    @Test
    public void test_membershipTableSwitchLeader() {
        MembershipTable m = new MembershipTable();
        MemberInfo info = new MemberInfo("Jennys-MacBook-Pro.local", 1314, "", true, true);
        m.put(1, info);
        info = new MemberInfo("Jennys-MacBook-Pro.local", 1315, "", false, true);
        m.put(2, info);
        info = new MemberInfo("Jennys-MacBook-Pro.local", 1316, "", false, false);
        m.put(3, info);
        m.switchLeaderShip(1,2);
        assertEquals(m.getLeaderID(), 2);

    }


    @Test
    public void test_membershipTableMarkDead() {
        MembershipTable m = new MembershipTable();
        MemberInfo info = new MemberInfo("Jennys-MacBook-Pro.local", 1314, "", true, true);
        m.put(1, info);
        info = new MemberInfo("Jennys-MacBook-Pro.local", 1315, "", false, true);
        m.put(2, info);
        info = new MemberInfo("Jennys-MacBook-Pro.local", 1316, "", false, false);
        m.put(3, info);
        assertEquals(m.getMemberInfo(1).isAlive, true);
        m.markDead(1);
        assertEquals(m.getMemberInfo(1).isAlive, false);
    }


    @Test
    public void test_membershipTableCancelLeadership() {
        MembershipTable m = new MembershipTable();
        MemberInfo info = new MemberInfo("Jennys-MacBook-Pro.local", 1314, "", true, true);
        m.put(1, info);
        info = new MemberInfo("Jennys-MacBook-Pro.local", 1315, "", false, true);
        m.put(2, info);
        info = new MemberInfo("Jennys-MacBook-Pro.local", 1316, "", false, false);
        m.put(3, info);
        assertEquals(m.getMemberInfo(1).isLeader, true);
        m.cancelLeadership(1);
        assertEquals(m.getMemberInfo(1).isAlive, false);
    }



    @Test
    public void test_membershipTableGetLeader() {
        MembershipTable m = new MembershipTable();
        MemberInfo info = new MemberInfo("Jennys-MacBook-Pro.local", 1314, "", true, true);
        m.put(1, info);
        info = new MemberInfo("Jennys-MacBook-Pro.local", 1315, "", false, true);
        m.put(2, info);
        info = new MemberInfo("Jennys-MacBook-Pro.local", 1316, "", false, false);
        m.put(3, info);
        assertEquals(m.getLeaderID(), 1);
    }



    @Test
    public void test_membershipTableCreateID() {
        MembershipTable m = new MembershipTable();
        MemberInfo info = new MemberInfo("Jennys-MacBook-Pro.local", 1314, "", true, true);
        m.put(1, info);
        info = new MemberInfo("Jennys-MacBook-Pro.local", 1315, "", false, true);
        m.put(2, info);
        info = new MemberInfo("Jennys-MacBook-Pro.local", 1316, "", false, false);
        m.put(3, info);
        int newId = m.createHighestId();
        assertEquals(newId, 4);
    }


    @Test
    public void test_failureDetection() {
        MembershipTable m = new MembershipTable();
        MemberInfo info = new MemberInfo("Jennys-MacBook-Pro.local", 1314, "", true, true);
        m.put(1, info);
        info = new MemberInfo("Jennys-MacBook-Pro.local", 1315, "", false, true);
        m.put(2, info);
        info = new MemberInfo("Jennys-MacBook-Pro.local", 1316, "", false, false);
        m.put(3, info);
        Connection conn = new Connection("Jennys-MacBook-Pro.local", 1315, true); // connection to broker 2
        Connection connLB = new Connection("Jennys-MacBook-Pro.local", 1130, true); // connection to LB
        HashMap<Integer, Connection> connMap = new HashMap<>();
        connMap.put(0, connLB);
        connMap.put(1, null);
        connMap.put(2, conn);
        //if in election
        FailureDetector fd  = new FailureDetector(m, 2, true, conn, 1, connMap, true);
        fd.run();
        assertEquals(fd.currentLeader, 1);
        assertEquals(fd.inElection, false);
        fd.announceNewLeadership();
        assertEquals(fd.currentLeader, 1);
    }


    @Test
    public void test_HearBeatListener() {
        MembershipTable m = new MembershipTable();
        MemberInfo info = new MemberInfo("Jennys-MacBook-Pro.local", 1314, "", true, true);
        m.put(1, info);
        info = new MemberInfo("Jennys-MacBook-Pro.local", 1315, "", false, true);
        m.put(2, info);
        info = new MemberInfo("Jennys-MacBook-Pro.local", 1316, "", false, false);
        m.put(3, info);
        Connection conn = new Connection("Jennys-MacBook-Pro.local", 1315, true); // connection to broker 2
        Connection connLB = new Connection("Jennys-MacBook-Pro.local", 1130, true); // connection to LB
        HashMap<Integer, Connection> connMap = new HashMap<>();
        connMap.put(0, connLB);
        connMap.put(1, null);
        connMap.put(2, conn);
        HeartBeatListener hbl = new HeartBeatListener(conn, m, 2, true, 1, connMap, false);
        Thread th = new Thread(hbl);
        th.start();
        assertEquals(hbl.currentLeaderBeforeMarkDead, 0);
        assertEquals(hbl.bq.getSize(), 0);
        assertEquals(hbl.membershipTable.getLeaderID(), 1);
        assertEquals(hbl.inElection, false);
    }


    @Test
    public void test_dataReceiver() {
        MembershipTable m = new MembershipTable();
        MemberInfo info = new MemberInfo("Jennys-MacBook-Pro.local", 1314, "", true, true);
        m.put(1, info);
        info = new MemberInfo("Jennys-MacBook-Pro.local", 1315, "", false, true);
        m.put(2, info);
        info = new MemberInfo("Jennys-MacBook-Pro.local", 1316, "", false, false);
        m.put(3, info);
        Connection conn = new Connection("Jennys-MacBook-Pro.local", 1315, true); // connection to broker 2
        Connection connLB = new Connection("Jennys-MacBook-Pro.local", 1130, true); // connection to LB
        HashMap<Integer, Connection> connMap = new HashMap<>();
        connMap.put(0, connLB);
        connMap.put(1, null);
        connMap.put(2, conn);

        Connection dataConn = new Connection("Jennys-MacBook-Pro.local", 1325, true); // connection to broker 2
        HashMap<Integer, Connection> dataConnMap = new HashMap<>();
        dataConnMap.put(1, dataConn);
        dataConnMap.put(2, null);

        Map<String, CopyOnWriteArrayList<ByteString>> topicMap = new HashMap<>();
        CopyOnWriteArrayList<ByteString> lst = new CopyOnWriteArrayList<ByteString>();
        String record = "2.177.12.140 - - [22/Jan/2019:03:56:22 +0330] \"GET /image/64500/productModel/1.1\" " +
                "\"https://www.zanbil.ir/m/product/33606/D8%AFHD-4K\" \"Mozilla/64.0 Firefox/64.0\" \"-\"";

        lst.add(ByteString.copyFrom(record.getBytes(StandardCharsets.UTF_8)));
        lst.add(ByteString.copyFrom(record.getBytes(StandardCharsets.UTF_8)));
        lst.add(ByteString.copyFrom(record.getBytes(StandardCharsets.UTF_8)));
        topicMap.put("image", lst);

        DataReceiver dr = new DataReceiver("Jennys-MacBook-Pro.local", 1141, conn, dataConnMap,
        true, 0, topicMap, m, 0);
        Thread th = new Thread(dr);
        th.start();

        LeaderBasedProducer p = new LeaderBasedProducer("Jennys-MacBook-Pro.local:1130");
        p.send(record.getBytes(StandardCharsets.UTF_8));
        p.send(record.getBytes(StandardCharsets.UTF_8));
        p.send(record.getBytes(StandardCharsets.UTF_8));
        assertEquals(dr.membershipTable.size(), 3);
        assertEquals(dr.counter, 0);
        assertEquals(dr.dataCounter, 0);
    }

    @Test
    public void test_broker(){
        try {
            server = new Server(brokerPort);
            //  System.out.println("A broker start listening on port: " + this.port + "...");
        } catch (IOException e) {
            e.printStackTrace();
        }
        Connection connection = this.server.nextConnection(); // calls accept on server socket to block
        //Connection connection = new Connection(brokerHostName, brokerPort);
        //  Project2.DistributedBroker.Receiver = new Project2.ReceiveProducerData(buffer, topicMapList, brokerID)
        byte[] buffer = connection.receive();
        System.out.println(buffer);
        assertEquals(LeaderBasedBroker.currentLeader, 1);

    }


    @Test
    public void test_consumer_subscribe(){
        Consumer consumer = new Consumer(LBLocation, topic, startingPosition);
        consumer.subscribe(topic, startingPosition);
        try { // every 3 sec request new data
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertEquals(0, consumer.getReceiverCounter());
    }


    @Test
    public void test_getHostname() {
        assertEquals(Utilities.getHostName(), "Jennys-MacBook-Pro.local");
    }

    //check args:
    @Test
    public void test_validateArgsBroker() {
        assertFalse(Utilities.validateArgsBroker(new String[]{}));
    }

    @Test
    public void test_validateArgsProducer() {
        assertFalse(Utilities.validateArgsProducer(new String[]{}));
        assertFalse(Utilities.validateArgsProducer(new String[]{"localhost:1000"}));
        assertTrue(Utilities.validateArgsProducer(new String[]{"localhost:1000", "test.txt"}));
    }

    @Test
    public void test_validateArgsLoadBalancer() {
        assertFalse(Utilities.validateArgsLoadBalancer(new String[]{"3 2"}));
        assertFalse(Utilities.validateArgsLoadBalancer(new String[]{"broker.json"}));
        assertTrue(Utilities.validateArgsLoadBalancer(new String[]{""}));
    }




}