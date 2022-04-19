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

    public DataReceiver(String name, int port, Connection conn, HashMap<Integer, Connection> dataConnMap,
                        boolean synchronous, int dataCounter, Map<String, CopyOnWriteArrayList<ByteString>> topicMap,
                        MembershipTable membershipTable) {
        this.name = name;
        this.port = port;
        this.conn = conn;
        brokerID = Utilities.getBrokerIDFromFile(name, String.valueOf(port), "files/brokerConfig.json");
        ackCount = new AtomicInteger(0);
        this.dataConnMap = dataConnMap;
        this.synchronous = synchronous;
        this.topicMap = topicMap;
        this.membershipTable = membershipTable;
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
                    System.out.println("\n *** New Connection coming in ***");
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
                                System.out.println(">>> Follower " + brokerID + " received data replication !!!");
                                if(synchronous) {
                                    //send ack back to leader:
                                    Acknowledgment.ack ack = Acknowledgment.ack.newBuilder()
                                            .setSenderType("ack").build();

                                    dataConnMap.get(membershipTable.getLeaderID()).send(ack.toByteArray());
                                    System.out.println(">>> sent ack back to the lead broker!!!!!!");
                                }

                                //store data
                                ByteString dataInBytes = m.getData();
                                Thread th = new Thread(new LeaderBasedReceiveProducerData(conn, dataInBytes, topicMap, dataCounter));
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
                            //send everything in topic map
//                                for (Map.Entry<String, CopyOnWriteArrayList<ByteString>> entry : topicMap.entrySet()) {
//                                    String topic = entry.getKey();
//                                    CopyOnWriteArrayList<ByteString> lst = entry.getValue();
//                                    for(int i = 0; i < lst.size(); i++) {
//                                        ByteString msg = lst.get(i);
//
//                                        String key = lst.;
//                                        MessageInfo.Message record = MessageInfo.Message.newBuilder()
//                                                .setTopic(topic)
//                                                .setKey(key)
//                                                .setValue(data)
//                                                .setPartition(partition)
//                                                .setOffset(offset)
//                                                .build();
//                                        conn.send(record.toByteArray());
//                                    }
//                                }
//

//                                Acknowledgment.ack response = Acknowledgment.ack.newBuilder()
//                                        .setSenderType("catchupData")
//                                        .setData(ByteString.copyFrom(topicMap.toString().getBytes(StandardCharsets.UTF_8)))
//                                        .build();
//
//                                conn.send(response.toByteArray());
                            //newBrokerDataRequest = true;
                            int peerId = Integer.parseInt(m.getLeadBrokerLocation());
                            AsynchronousReplication asynchronousReplication = new AsynchronousReplication(membershipTable, buffer, brokerID, dataConnMap, peerId, topicMap, conn);//use data connections
                                //asynchronousReplication.run();
                            Thread rep = new Thread(asynchronousReplication);
                            rep.start();
                        }
                            else if(m.getSenderType().equals("catchupData")){
                                System.out.println("########## receive and store catchup data");
                                //store data
                                ByteString dataInBytes = m.getData();
                                System.out.println(dataInBytes.toByteArray().toString());
//                                Thread th = new Thread(new LeaderBasedReceiveProducerData(conn, dataInBytes, topicMap, dataCounter));
//                                th.start();
//                                try {
//                                    th.join();
//                                } catch (InterruptedException e) {
//                                    e.printStackTrace();
//                                }

                            }



                            dataCounter++;
                            counter++;
                        }
                    }
                }
            }
        }
    }
}


