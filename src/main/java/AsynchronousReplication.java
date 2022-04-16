import com.google.protobuf.ByteString;
import dsd.pubsub.protos.Acknowledgment;
import dsd.pubsub.protos.MessageInfo;
import dsd.pubsub.protos.Resp;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AsynchronousReplication implements Runnable{
    MembershipTable membershipTable;
    byte[] buffer;
    int brokerID;
    HashMap<Integer, Connection> dataConnMap;
    private static ExecutorService executor;

    public AsynchronousReplication(MembershipTable membershipTable, byte[] buffer, int brokerID, HashMap<Integer, Connection> dataConnMap) {
        this.membershipTable = membershipTable;
        this.buffer = buffer;
        this.brokerID = brokerID;
        this.dataConnMap = dataConnMap;
        //executor = Executors.newSingleThreadExecutor();
        executor = Executors.newFixedThreadPool(10);
    }

    @Override
    public void run() {
        Runnable replication = () -> {
            for (int id : membershipTable.getKeys()) {
                if (membershipTable.getMemberInfo(id).isAlive && id != brokerID) {
                    //draft data
//                    ByteString b = ByteString.copyFrom(buffer);
//                    MessageInfo.Message record = MessageInfo.Message.newBuilder()
//                            .setValue(b)
//                            .build();
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
