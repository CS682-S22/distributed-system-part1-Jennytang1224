import dsd.pubsub.protos.Resp;
import dsd.pubsub.protos.Response;

import java.util.HashMap;

public class FailureDetector {
    int peerID;
    MembershipTable membershipTable;
    boolean inElection;
    Connection conn;
    int brokerID;
    private HashMap<Integer, Connection> connMap;
    int peerCounterForElection;
    int winnerId;


    public FailureDetector(MembershipTable membershipTable, int peerID, boolean inElection,
                           Connection conn, int brokerID, HashMap<Integer, Connection> connMap){
        this.membershipTable = membershipTable;
        this.peerID = peerID;
        this.inElection = inElection;
        this.conn = conn;
        this.brokerID = brokerID;
        this.connMap = connMap;
    }

    public void run() {
        if(!inElection){ //if expecting heartbeat, but nothing
            System.out.println("exceed timeout, assume peer: " + peerID + " is dead ");
            if (membershipTable.getMemberInfo(peerID).isLeader) { // leader is dead
                //if peerid is leader, bully (send initial election msg and wait for election response)
                BullyElection bully = new BullyElection(brokerID, membershipTable, connMap, conn);
                bully.run();
                peerCounterForElection = bully.getPeerCounter();
                System.out.println("~~~ # of peers that me (broker " + brokerID + ") send election msg to");

                //membershipTable.switchLeaderShip(peerID, peerID + 1); // naively choosing next smallest id, change later
            } else { // if peerid is follower, update table  - mark dead
                membershipTable.markDead(peerID);
                //in the table, check if there's any lower id broker than me is alive, if not, im the leader
                for (int i = 1; i <= connMap.size(); i++) {
                    int olderLeader = membershipTable.getLeaderID();
                    if ((membershipTable.getMemberInfo(i).isAlive) && (i < brokerID) && (i != olderLeader)) {
                        // there exists a more qualified broker than me to be the leader
                        System.out.println("im out..waiting for leader announcement..");
                    }else{ // if no such broker exists, im the new leader!
                        //update my table, make self as the leader
                        membershipTable.switchLeaderShip(olderLeader, brokerID);

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
                        inElection = false;

                    }
                }
            }

            System.out.println("~~~~~~~~~~~~~~~~~~table after " + peerID + " failed to return heartbeat msg");
            membershipTable.print();
            System.out.println(" ");
        }

        else{// if expecting election msg, but nothing
            // mark the peer is dead
            membershipTable.markDead(peerID);
        }
    }


    public boolean getElectionStatus(){
        return inElection;
    }
}
