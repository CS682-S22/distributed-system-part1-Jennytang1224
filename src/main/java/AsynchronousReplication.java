import com.google.protobuf.ByteString;
import dsd.pubsub.protos.Acknowledgment;
import dsd.pubsub.protos.MessageInfo;
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
    boolean clear;

    public AsynchronousReplication(MembershipTable membershipTable, byte[] buffer, int brokerID,
                                   HashMap<Integer, Connection> dataConnMap, int peerID,
                                   Map<String, CopyOnWriteArrayList<ByteString>> topicMap, Connection conn, boolean clear) {
        this.membershipTable = membershipTable;
        this.buffer = buffer;
        this.brokerID = brokerID;
        this.dataConnMap = dataConnMap;
        executor = Executors.newFixedThreadPool(10);
        this.peerID = peerID;
        this.topicMap = topicMap;
        this.conn = conn;
        this.clear = clear;
    }

    @Override
    public void run() {
        if (peerID != -1) { // for new broker catch up with data, send record by record
            int num = 0;
            if(clear){
                num = 1;
            }
            if(topicMap != null && topicMap.size() != 0 ) {
                String topic;
                ByteString data;
                for(Map.Entry<String, CopyOnWriteArrayList<ByteString>> entry: topicMap.entrySet()){
                    topic = entry.getKey();
                    for(int i = 0; i < entry.getValue().size(); i++){
                        data = entry.getValue().get(i);
                        MessageInfo.Message record = MessageInfo.Message.newBuilder()
                                .setTopic(topic)
                                .setValue(data)
                                .build();

                        Acknowledgment.ack catchupData = Acknowledgment.ack.newBuilder()
                                .setSenderType("catchupData")
                                .setData(ByteString.copyFrom(record.toByteArray()))
                                .setNum(num)
                                .build();

                        (dataConnMap.get(peerID)).send(catchupData.toByteArray()); // send data message
                        System.out.println("######## sending catch up data to " + peerID);
                    }
                }
            }
            else{
                System.out.println("####### nothing in my topic map yet");
            }

        } else { // async replication
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
            executor.execute(replication);
        }
    }

}
