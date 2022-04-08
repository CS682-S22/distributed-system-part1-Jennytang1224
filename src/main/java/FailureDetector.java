public class FailureDetector {
    int peerID;
    MembershipTable membershipTable;

    public FailureDetector(MembershipTable membershipTable, int peerID){
        this.membershipTable = membershipTable;
        this.peerID = peerID;

    }

    public void run() {

        // if more than number of retires, update table as the broker failed, and stop sending
        System.out.println("exceed timeout, assume peer: " + peerID + " is dead ");
        //remove broker from the table
        if (membershipTable.getMemberInfo(peerID).isLeader) { // leader is dead
            // bully election .. need another class
//                        inElection = true;
//                        Thread th = new Thread(new BullyElection(brokerID, membershipTable, connMap)); // oldLeader, membershipTable
//                        th.start();
//                        try {
//                            th.join();
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }

            membershipTable.switchLeaderShip(peerID, peerID - 1); // naively choosing next smallest id, change later
        } else { //follower is dead, mark dead
            membershipTable.markDead(peerID);
        }

        System.out.println("~~~~~~~~~~~~~~~~~~table after failed to return heartbeat msg");
        membershipTable.print();
        System.out.println(" ");
    }
}
