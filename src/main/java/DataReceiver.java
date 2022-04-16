import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.Acknowledgment;
import dsd.pubsub.protos.PeerInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * inner class Receiver
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

    public DataReceiver(String name, int port, Connection conn, HashMap<Integer, Connection> dataConnMap,
                        boolean synchronous, int dataCounter, Map<String, CopyOnWriteArrayList<ByteString>> topicMap) {
        this.name = name;
        this.port = port;
        this.conn = conn;
        brokerID = Utilities.getBrokerIDFromFile(name, String.valueOf(port), "files/brokerConfig.json");
        ackCount = new AtomicInteger(0);
        this.dataConnMap = dataConnMap;
        this.synchronous = synchronous;
        this.topicMap = topicMap;
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
                                System.out.println(">>> Follower " + brokerID + " received data replication !!!");
                                if(synchronous) {
                                    //send ack back to leader:
                                    Acknowledgment.ack ack = Acknowledgment.ack.newBuilder()
                                            .setSenderType("ack").build();

                                    dataConnMap.get(1).send(ack.toByteArray());
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

                            dataCounter++;
                            counter++;
                        }
                    }
                }
            }
        }
    }
}


