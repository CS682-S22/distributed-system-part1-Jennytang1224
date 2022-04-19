import com.google.protobuf.ByteString;
import dsd.pubsub.protos.Acknowledgment;
import dsd.pubsub.protos.MessageInfo;

import java.sql.BatchUpdateException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class SynchronousReplication implements Runnable{
    MembershipTable membershipTable;
    byte[] buffer;
    int brokerID;
    HashMap<Integer, Connection> dataConnMap;
    private static ExecutorService executor;
    AtomicInteger numOfAckNeeded;


    public SynchronousReplication(MembershipTable membershipTable, byte[] buffer, int brokerID, HashMap<Integer, Connection> dataConnMap) {
        this.membershipTable = membershipTable;
        this.buffer = buffer;
        this.brokerID = brokerID;
        this.dataConnMap = dataConnMap;
        //executor = Executors.newSingleThreadExecutor();
        //executor = Executors.newFixedThreadPool(10);
        numOfAckNeeded = new AtomicInteger(0);
    }


    @Override
    public void run() {
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
//
//                    try {
//                        Thread.sleep(100);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                    if(membershipTable.getMemberInfo(id).isAlive) {
//                        numOfAckNeeded.getAndIncrement();
//                    }
                (dataConnMap.get(id)).send(record.toByteArray()); // send data message
                System.out.println("this lead broker sent sync replication of data to follower " + id);
            }
        }

//        try { // to get tha ack count
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        for (int id : membershipTable.getKeys()) {
//            if (membershipTable.getMemberInfo(id).isAlive && id != brokerID) {
//                //if(membershipTable.getMemberInfo(id).isAlive) {
//                        numOfAckNeeded.getAndIncrement();
//                 //   }
//            }
//        }
    }

}
