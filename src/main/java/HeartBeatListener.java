import dsd.pubsub.protos.HeartBeatMessage;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HeartBeatListener implements Runnable{
    Connection conn;
    MembershipTable membershipTable;
    private CS601BlockingQueue<HeartBeatMessage.HeartBeat> bq;
    private ExecutorService executor;
    int delay = 1000;
    int retires = 3;
    HeartBeatMessage.HeartBeat f;
    int peerID;
    boolean sending;


    public HeartBeatListener(Connection conn, MembershipTable membershipTable, int peerID, boolean sending ) {
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

  //  @Override
    public void run() {
      //  while(receiving){
        Runnable add = () -> {
            try {
                byte[] result = conn.receive();
                if (result != null) {
                    bq.put(HeartBeatMessage.HeartBeat.parseFrom(result));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        };

        // timeout for heartbeat response
        executor.execute(add);
        int replyingBrokerId = -1;
        f = bq.poll(delay * retires);
        if (f != null) { // received a pack within in timeout, send a new heartbeat
            replyingBrokerId = f.getSenderID();
//                    if(replyingBrokerId != peerID){
//                        System.out.println("!!!!! receiving from different peer");
//                    }
            System.out.println("receiving heartbeat response from peer: " + replyingBrokerId);
            // retryCount = 1; // reset

        } else { // not receive response within timeout
            // failure detection
            FailureDetector failureDetector = new FailureDetector(membershipTable, peerID);
            failureDetector.run();

            //stop the connection since the peer is dead
            sending = false;
        }

        // else if within num of retires, send same heart beat again, go back to while loop


    }
}
