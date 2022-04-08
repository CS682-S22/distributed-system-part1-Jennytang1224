import dsd.pubsub.protos.Resp;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BullyElection {
    int brokerId;
    MembershipTable membershipTable;
    HashMap<Integer, Connection> connMap;
    private CS601BlockingQueue<Resp.Response> bq;
    private ExecutorService executor;
    Connection conn;
    int winnerId = -1;
    int peerCounter = 0; // # of peers you send election msg to


    public BullyElection(int brokerId, MembershipTable membershipTable, HashMap<Integer, Connection> connMap, Connection conn) {
        this.brokerId = brokerId;
        this.membershipTable = membershipTable;
        this.connMap = connMap;
        this.bq = new CS601BlockingQueue<>(1);
        this.executor = Executors.newSingleThreadExecutor();
        this.conn = conn;
    }

    public int getPeerCounter(){
        return peerCounter;
    }


    public void run() {
        //this broker send election msg to all lower-id brokers and wait for election response
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (int peerID : connMap.keySet()) {
           if ((membershipTable.getMemberInfo(peerID).isAlive) && (peerID < brokerId)) {
                //get connection between this broker and the other broker
          //  if ((membershipTable.getMemberInfo(peerID).isAlive)
                Connection conn = connMap.get(peerID);

                //draft election msg
                Resp.Response electionMessage = Resp.Response.newBuilder()
                        .setType("election")
                        .setSenderID(brokerId)
                        .setWinnerID(winnerId).build();
                conn.send(electionMessage.toByteArray()); // send election message
                peerCounter++;
                System.out.println(peerID);
                //if no any response, meaning this broker is the lowest-id living broker,
                // claim leadership notify others
//                if (!isThereHigherIDBroker) {
//                    winnerId = brokerId;
//                    for (int brokerID : connMap.keySet()) {
//                        if ((membershipTable.getMemberInfo(brokerID).isAlive)) { // only notify all living brokers
//                            //get connection between this broker and the
//                            Connection conn = connMap.get(brokerID);
//                            Response.OneResponse electionMessage = Response.OneResponse.newBuilder()
//                                    .setElection(Response.Election.newBuilder()
//                                            .setSenderID(brokerId)
//                                            .setWinnerID(winnerId).build()).build();
//                            conn.send(electionMessage.toByteArray()); // send election message notify i am the winner
//                        }
//                    }
//                }
            }

        }
     //   System.out.println("~~~ in BULLY # of peers that me (broker " + brokerId + ") send election msg to " + peerCounter);


    }
}
