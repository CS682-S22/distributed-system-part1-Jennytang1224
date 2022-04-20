import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.Acknowledgment;
import dsd.pubsub.protos.MessageInfo;
import dsd.pubsub.protos.PeerInfo;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * receiver only receives replicated data
 */
class DataReceiver implements Runnable {
    private String name;
    private int port;
    private Connection conn;
    boolean receiving = true;
    int counter = 0;
    private String type;
    int brokerID;
    int peerID;
    static AtomicInteger ackCount;
    String peerHostName;
    int peerPort;
    static HashMap<Integer, Connection> dataConnMap;
    boolean synchronous;
    static Map<String, CopyOnWriteArrayList<ByteString>> topicMap;
    int dataCounter;
    MembershipTable membershipTable;
    Connection newBrokerConn;
    boolean newBrokerDataRequest;
    int catchupDataCounter = 0;
    static int clearCounter;


    public DataReceiver(String name, int port, Connection conn, HashMap<Integer, Connection> dataConnMap,
                        boolean synchronous, int dataCounter, Map<String, CopyOnWriteArrayList<ByteString>> topicMap,
                        MembershipTable membershipTable, int clearCounter) {
        this.name = name;
        this.port = port;
        this.conn = conn;
        brokerID = Utilities.getBrokerIDFromFile(name, String.valueOf(port), "files/brokerConfig.json");
        ackCount = new AtomicInteger(0);
        this.dataConnMap = dataConnMap;
        this.synchronous = synchronous;
        this.topicMap = topicMap;
        this.membershipTable = membershipTable;
        this.clearCounter = clearCounter;
    }


    @Override
    public void run() {
        PeerInfo.Peer p = null;
        Acknowledgment.ack m = null;
        while (receiving) {
            byte[] buffer = conn.receive();
            if (buffer == null || buffer.length == 0) {
            }
            else { // buffer not null
                if (counter == 0) { // first msg is peerinfo
                    try {
                        p = PeerInfo.Peer.parseFrom(buffer);
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }

                    type = p.getType(); // consumer or producer
                    System.out.println("\n *** New Data Connection coming in ***");
                    System.out.println("Peer type: " + type);
                    peerHostName = p.getHostName();
                    peerPort = p.getPortNumber();
                    peerID = Utilities.getBrokerIDFromFile(peerHostName, String.valueOf(peerPort), "files/brokerConfig.json");
                    //   System.out.println("~~~~~~~    " + peerHostName + ":" + peerPort + " " + peerID );
                    if (type.equals("broker")) { // other brokers
                        System.out.println("this broker on data port has connected to BROKER " + peerHostName + ":" + peerPort + "\n");
                        counter++;
                    }

                } else { // when receiving leader data
                    if (type.equals("broker")) { //leader sending data copy

                        try{
                            m = Acknowledgment.ack.parseFrom(buffer);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        if ( m != null) {
                            if(m.getSenderType().equals("data")){
                                //newBrokerDataRequest = false;
                                System.out.println("\n >>> Follower " + brokerID + " received data replication !!!");
                                if(synchronous) {
                                    //send ack back to leader:
                                    Acknowledgment.ack ack = Acknowledgment.ack.newBuilder()
                                            .setSenderType("ack").build();

                                    dataConnMap.get(membershipTable.getLeaderID()).send(ack.toByteArray());
                                    System.out.println(" >>> sent ack back to the lead broker!!!!!!");
                                }

                                //store data
                                ByteString dataInBytes = m.getData();
                                Thread th = new Thread(new LeaderBasedReceiveProducerData(conn, dataInBytes, topicMap, dataCounter, false));
                                th.start();
                                try {
                                    th.join();
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }

                            } else if(m.getSenderType().equals("ack")){ //only lead broker will receive ack
                                System.out.println("~~~~~~~~~AN ACK received ");
                                ackCount.getAndIncrement();

                            }
                            else if(m.getSenderType().equals("catchup")) { // new broker join and wants to catchup on data
                                System.out.println("connection when request arrive: " + conn);
                                int peerId = Integer.parseInt(m.getLeadBrokerLocation());
                                boolean clear = false;
                                AsynchronousReplication asynchronousReplication =
                                        new AsynchronousReplication(membershipTable, buffer, brokerID, dataConnMap, peerId, topicMap, conn, clear);
                                Thread rep = new Thread(asynchronousReplication);
                                rep.start();
                            }
                            else if(m.getSenderType().equals("catchupData")){
                               // System.out.println("########## receive and store catchup data");
                                //store data
                                ByteString dataInBytes = m.getData();
                                int num = m.getNum();
                                boolean clear = true;
                                if(num == 0){
                                    clear = false;
                                }
                               // System.out.println("$$$$$$$$$$$$$$$$$$$ clear counter: " + clearCounter);
                                if(clear && clearCounter == 0){
                                    topicMap.clear();
                                    clearCounter++;
                                    System.out.println("~~~ original data got removed");
                                }
                                Thread th = new Thread(new LeaderBasedReceiveProducerData(conn, dataInBytes, topicMap, catchupDataCounter, clear));
                                th.start();
                                try {
                                    th.join();
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                            else if(m.getSenderType().equals("requestNewestData")){
                                System.out.println("receive the request from new leader to compare data...");
                                int peerId = Integer.parseInt(m.getLeadBrokerLocation());
                                int peerMapSize = m.getNum();
                                int currentMapSize = 0;
                                for(Map.Entry<String, CopyOnWriteArrayList<ByteString>> entry : topicMap.entrySet()) {
                                    for(int i = 0; i < entry.getValue().size(); i++){
                                        currentMapSize++;
                                    }
                                }
                                System.out.println("My Size: " + currentMapSize + ", Peer Size: " + peerMapSize );
                                if(currentMapSize > peerMapSize){ // leader doesn't have newest data
                                    boolean clear = true; // clear existing data
                                    System.out.println("############### sending newest data to the leader...");
                                    AsynchronousReplication asynchronousReplication =
                                            new AsynchronousReplication(membershipTable, buffer, brokerID, dataConnMap, peerId, topicMap, conn, clear);
                                    Thread rep = new Thread(asynchronousReplication);
                                    rep.start();
                                }

                            }

                            dataCounter++;
                            counter++;
                            catchupDataCounter++;
                        }
                    }
                }
            }
        }
    }
}


