import dsd.pubsub.protos.Resp;
import dsd.pubsub.protos.Response;

import java.util.HashMap;

public class FailureDetector {
    int peerID;
    MembershipTable membershipTable;
    volatile boolean inElection;
    Connection conn;
    int brokerID;
    private HashMap<Integer, Connection> connMap;
    int peerCounterForElection;
    int winnerId;
    int currentLeader;
    boolean isThereLowerIDBroker = false;


    public FailureDetector(MembershipTable membershipTable, int peerID, boolean inElection,
                           Connection conn, int brokerID, HashMap<Integer, Connection> connMap){
        this.membershipTable = membershipTable;
        this.peerID = peerID;
        this.inElection = inElection;
        this.conn = conn;
        this.brokerID = brokerID;
        this.connMap = connMap;
    }

    public int getCurrentLeaderBeforeMarkDead(){
        return currentLeader;
    }


    public void run() {
        currentLeader = membershipTable.getLeaderID();
        if(!inElection){ //if expecting heartbeat, but nothing
        //    if(peerID != brokerID && membershipTable.getMemberInfo(peerID).isAlive) {
                System.out.println("exceed timeout, assume peer: " + peerID + " is dead ");
         //   }
            if (membershipTable.getMemberInfo(peerID).isLeader) { // leader is dead
                //if peerid is leader, bully (send initial election msg and wait for election response)
                membershipTable.markDead(peerID);
                membershipTable.cancelLeadership(peerID);

                BullyElection bully = new BullyElection(brokerID, membershipTable, connMap, conn);
                bully.run();
                peerCounterForElection = bully.getPeerCounter();
                System.out.println("~~~ # of peers that me (broker " + brokerID + ") send election msg to: " + peerCounterForElection);
                inElection = true;
               // membershipTable.switchLeaderShip(peerID, peerID + 1); // naively choosing next smallest id, change later
            } else { // if peerid is follower, update table  - mark dead
                membershipTable.markDead(peerID);
                inElection = false;
            }

            System.out.println("~~~~~~~~~~~~~~~~~~table after " + peerID + " failed to return heartbeat msg");
            membershipTable.print();
            System.out.println(" ");
        }

        else{ // if expecting election msg, but nothing
            System.out.println("if expecting election msg, but nothing");
            membershipTable.markDead(peerID); // mark the peer is dead
            //   in the table, check if there's any lower id broker than me is alive, if not, im the leader
            for (int i = 1; i <= connMap.size(); i++) {
                // int olderLeader = membershipTable.getLeaderID();
                if ((membershipTable.getMemberInfo(i).isAlive) && (i < brokerID)) {
                    // there exists a more qualified broker than me to be the leader
                    System.out.println("im out..waiting for leader announcement from other broker..");
                    isThereLowerIDBroker = true;
                    inElection = true;
                    break;
                }
            }

            if(!isThereLowerIDBroker) { // if no such broker exists, im the new leader!
                announceNewLeadership();
//                inElection = false;
            }
        }
    }

    public void announceNewLeadership(){
        System.out.println("update table and announce im the new leader");
        //update my table, make self as the leader
        membershipTable.switchLeaderShip(currentLeader, brokerID);

        // notify everyone im the new leader
        winnerId = brokerID;
        for (int peerID : connMap.keySet()) {
            if ((membershipTable.getMemberInfo(peerID).isAlive)) { // only notify all living brokers
                //get connection between this broker and the
                Connection conn = connMap.get(peerID);
                Resp.Response electionMessage = Resp.Response.newBuilder()
                        .setType("election")
                        .setSenderID(brokerID)
                        .setWinnerID(winnerId).build();
                conn.send(electionMessage.toByteArray()); // send election message notify i am the winner
            }
        }
    }

    public boolean getElectionStatus(){
        return inElection;
    }
}
