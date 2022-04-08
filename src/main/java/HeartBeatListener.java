import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.HeartBeatMessage;
import dsd.pubsub.protos.Resp;
import dsd.pubsub.protos.Response;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class HeartBeatListener implements Runnable{
    Connection conn;
    MembershipTable membershipTable;
    private CS601BlockingQueue<Resp.Response> bq;
    private ExecutorService executor;
    int delay = 1000;
    int retires = 3;
    Response.OneResponse f;
    int peerID;
    boolean sending;
    boolean inElection = false;



    public HeartBeatListener(Connection conn, MembershipTable membershipTable, int peerID, boolean sending) {
       this.conn = conn;
       this.membershipTable = membershipTable;
       this.peerID = peerID;
       this.sending = sending;
       this.executor = Executors.newSingleThreadExecutor();
       this.bq = new CS601BlockingQueue<>(1);
    }

    public boolean getSending(){
        return this.sending;
    }

    //after sending out hearbeat msg -> expecting heartbeat response
    //if no hb response from leader, enter election and send initial election msg, wait for election response or decision
    @Override
    public void run() {

        Resp.Response f;
        Response.HeartBeat heartBeat;
        Response.Election election;

        Runnable add = () -> {
            try {
                byte[] result = conn.receive();
                if (result != null) {
                    bq.put(Resp.Response.parseFrom(result));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        };

        executor.execute(add);
        int replyingBrokerId = -1;
        f = bq.poll(delay * retires);

        // if there's response within timeout
        if (f != null) {
            //check if f is heartbeat or election message
            if (f.getType().equals("heartbeat")){
                inElection = false;
            } else if (f.getType().equals("election")) {
                inElection = true;
            } else{
                System.out.println("wrong type");
            }

            if(!inElection) {// if its heartbeat response
              //  heartBeat = f.getHeartBeat();
                replyingBrokerId = f.getSenderID();
                System.out.println("receiving heartbeat response from peer: " + replyingBrokerId);

            }
            else {// if its election response
                int senderId = f.getSenderID();
                int newLeader = f.getWinnerID();

                if (newLeader == -1) {// if winner is -1 ... its a simple election response, still in election
                    System.out.println("In Election, receiving election msg from broker " + senderId);
                } else { // if winner is not -1 ... we have a winner
                    System.out.println("new leader id:" + newLeader);
                    int oldLeader = membershipTable.getLeaderID();
                    System.out.println("old leader id:" + oldLeader);
                    if (oldLeader != -1) { // there's a leader
                        membershipTable.switchLeaderShip(oldLeader, newLeader);//update new leader
                    } else {
                        System.out.println("weird ... no current leader right now");
                    }
                    inElection = false; // election ended on my end
                    System.out.println("election ended");
                }
            }

        // if no response within timeout
        } else {
            // failure detection
            FailureDetector failureDetector = new FailureDetector(membershipTable, peerID, inElection, conn);
            failureDetector.run();

            //stop the connection since the peer is dead
            sending = false;
        }

        // else if within num of retires, send same heart beat again, go back to while loop


    }
}
