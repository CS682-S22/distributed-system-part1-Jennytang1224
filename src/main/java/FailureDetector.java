public class FailureDetector {
    int peerID;
    MembershipTable membershipTable;
    boolean inElection;
    Connection conn;

    public FailureDetector(MembershipTable membershipTable, int peerID, boolean inElection, Connection conn){
        this.membershipTable = membershipTable;
        this.peerID = peerID;
        this.inElection = inElection;
        this.conn = conn;
    }

    public void run() {
        if(!inElection){ //if expecting heartbeat, but nothing
            System.out.println("exceed timeout, assume peer: " + peerID + " is dead ");
            if (membershipTable.getMemberInfo(peerID).isLeader) { // leader is dead
                //if peerid is leader, send initial election msg and wait for election response

                // bully election .. need another class
//                        inElection = true;
//                        Thread th = new Thread(new BullyElection(brokerID, membershipTable, connMap)); // oldLeader, membershipTable
//                        th.start();
//                        try {
//                            th.join();
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }

                membershipTable.switchLeaderShip(peerID, peerID + 1); // naively choosing next smallest id, change later
            } else { // if peerid is follower, update table  - mark dead
                membershipTable.markDead(peerID);
            }

            System.out.println("~~~~~~~~~~~~~~~~~~table after " + peerID + " failed to return heartbeat msg");
            membershipTable.print();
            System.out.println(" ");
        }
//
//        else{// if expecting election msg, but nothing
//            // mark the peer is dead
//            membershipTable.markDead(peerID);
//        }

    }
}
