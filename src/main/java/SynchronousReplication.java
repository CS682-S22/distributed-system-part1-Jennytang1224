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
    AtomicInteger numOfAckNeeded;
    boolean isFailure;
    int messageCounter;



    public SynchronousReplication(MembershipTable membershipTable, byte[] buffer, int brokerID, HashMap<Integer, Connection> dataConnMap, boolean isFailure, int messageCounter) {
        this.membershipTable = membershipTable;
        this.buffer = buffer;
        this.brokerID = brokerID;
        this.dataConnMap = dataConnMap;
        numOfAckNeeded = new AtomicInteger(0);
        this.isFailure = isFailure;
        this.messageCounter = messageCounter;
    }


    @Override
    public void run() {
        //get id for next leader
        int nextLeaderID = membershipTable.getLeaderID() + 1;

        for (int id : membershipTable.getKeys()) {
            if (membershipTable.getMemberInfo(id).isAlive && id != brokerID) {
                //draft data
                Acknowledgment.ack record = Acknowledgment.ack.newBuilder()
                        .setSenderType("data")
                        .setData(ByteString.copyFrom(buffer))
                        .build();
                if(!isFailure) { // is no failure
                    (dataConnMap.get(id)).send(record.toByteArray()); // send data message
                    System.out.println("this lead broker sent sync replication of data to follower " + id);
                }else{ // there's a failure
                    // creating failure: fail to replicate one message to a follower(new leader)
                    if(messageCounter == 10 && id == nextLeaderID){ // do not send
                        System.out.println("***[INJECT FAILURE: purposely not sending last message to follower " + nextLeaderID + "]");
                    }else{
                        (dataConnMap.get(id)).send(record.toByteArray()); // send data message
                        System.out.println("this lead broker sent sync replication of data to follower " + id);
                    }
                }
            }
        }


    }

}
