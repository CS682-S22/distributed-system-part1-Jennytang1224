import com.google.protobuf.ByteString;
import dsd.pubsub.protos.Acknowledgment;
import dsd.pubsub.protos.MessageInfo;
import dsd.pubsub.protos.Resp;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AsynchronousReplication implements Runnable{
    MembershipTable membershipTable;
    byte[] buffer;
    int brokerID;
    HashMap<Integer, Connection> dataConnMap;
    private static ExecutorService executor;
    int peerID;
    Map<String, CopyOnWriteArrayList<ByteString>> topicMap;
    Connection conn;

    public AsynchronousReplication(MembershipTable membershipTable, byte[] buffer, int brokerID, HashMap<Integer, Connection> dataConnMap, int peerID, Map<String, CopyOnWriteArrayList<ByteString>> topicMap, Connection conn ) {
        this.membershipTable = membershipTable;
        this.buffer = buffer;
        this.brokerID = brokerID;
        this.dataConnMap = dataConnMap;
        //executor = Executors.newSingleThreadExecutor();
        executor = Executors.newFixedThreadPool(10);
        this.peerID = peerID;
        this.topicMap = topicMap;
        this.conn = conn;
    }

    @Override
    public void run() {
        if (peerID != -1) { // for new broker catch up with data

            Acknowledgment.ack record = Acknowledgment.ack.newBuilder()
                    .setSenderType("catchupData")
                    .setData(ByteString.copyFrom(topicMap.toString().getBytes(StandardCharsets.UTF_8)))
                    .build();
            dataConnMap.get(peerID).send(record.toByteArray()); // send data message
            System.out.println("######## sending catch up data to " + peerID);


        } else {
            Runnable replication = () -> {
                for (int id : membershipTable.getKeys()) {
                    if (membershipTable.getMemberInfo(id).isAlive && id != brokerID) {
                        Acknowledgment.ack record = Acknowledgment.ack.newBuilder()
                                .setSenderType("data")
                                .setData(ByteString.copyFrom(buffer))
                                .build();
                        (dataConnMap.get(id)).send(record.toByteArray()); // send data message
                        System.out.println("this lead broker sent Asynchronous replication of data to follower " + id);
                    }
                }
            };
            //    synchronized (this) {
            executor.execute(replication);
            //   }
        }
    }

}
