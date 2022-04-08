import dsd.pubsub.protos.ElectionMessage;
import dsd.pubsub.protos.HeartBeatMessage;
import dsd.pubsub.protos.Response;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BullyElection {
    int brokerId;
    MembershipTable membershipTable;
    HashMap<Integer, Connection> connMap;
    int retires = 3;
    private CS601BlockingQueue<Response.OneResponse> bq;
    private ExecutorService executor;
    boolean isThereHigherIDBroker = false;



    public BullyElection(int brokerId, MembershipTable membershipTable, HashMap<Integer, Connection> connMap) {
        this.brokerId = brokerId;
        this.membershipTable = membershipTable;
        this.connMap = connMap;
        this.bq = new CS601BlockingQueue<>(1);
        this.executor = Executors.newSingleThreadExecutor();

        // while listening if receive election msg from lower id broker, respond, start it's own election process
        // receiver thread

    }


    public void run() {
        //this broker send all higher-id brokers msg and wait for response
        int winnerId = -1;
        for (int peerID: connMap.keySet()){
            if((membershipTable.getMemberInfo(peerID).isAlive) && (peerID > brokerId)) {

                //get connection between this broker and the other broker
                Connection conn = connMap.get(peerID);

                //draft election msg
                Runnable add = () -> {
                    try {
                        byte[] result = conn.receive();
                        if (result != null) {
                            bq.put(Response.OneResponse.parseFrom(result));
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                };
                Response.OneResponse electionMessage = Response.OneResponse.newBuilder()
                        .setElection(Response.Election.newBuilder()
                        .setSenderID(brokerId)
                        .setWinnerID(winnerId).build()).build();
                conn.send(electionMessage.toByteArray()); // send election message

                // timeout for election response
                executor.execute(add);
                Response.OneResponse f;
                int replyingBrokerId = -1;
                f = bq.poll(1000 * retires);
                Response.Election e = f.getElection();

                if (e != null) { // received a pack within  timeout
                    replyingBrokerId = e.getSenderID();
                    System.out.println("received election response from broker: " + replyingBrokerId);
                    isThereHigherIDBroker = true;
                    break; //if there's one response, this broker will stop election and wait for notification
                }
                else{
                    // the receiver broker is dead, update the map
                    membershipTable.markDead(peerID);
                }
            }
        }

        //if no any response, meaning this broker is the highest-id living broker,
        // claim leadership notify others
        if(!isThereHigherIDBroker){
            winnerId = brokerId;
            for (int brokerID: connMap.keySet()) {
                if ((membershipTable.getMemberInfo(brokerID).isAlive)) { // only notify all living brokers
                    //get connection between this broker and the
                    Connection conn = connMap.get(brokerID);
                    Response.OneResponse electionMessage = Response.OneResponse.newBuilder()
                            .setElection(Response.Election.newBuilder()
                                    .setSenderID(brokerId)
                                    .setWinnerID(winnerId).build()).build();
                    conn.send(electionMessage.toByteArray()); // send election message notify i am the winner
                }
            }
        }
    }


    //inner class for receiver
}
