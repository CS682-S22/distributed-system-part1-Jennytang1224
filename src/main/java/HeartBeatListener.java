import dsd.pubsub.protos.Resp;
import dsd.pubsub.protos.Response;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class HeartBeatListener implements Runnable {
    Connection conn;
    MembershipTable membershipTable;
    private CS601BlockingQueue<Resp.Response> bq;
    private ExecutorService executor;
    int delay = 1000;
    int retires = 3;
    int peerID;
    volatile boolean sending;
    volatile boolean inElection;
    int brokerID;
    private HashMap<Integer, Connection> connMap;
    int currentLeaderBeforeMarkDead;
    boolean listening = true;


    public HeartBeatListener(Connection conn, MembershipTable membershipTable, int peerID, boolean sending,
                             int brokerID, HashMap<Integer, Connection> connMap, boolean inElection) {
        this.conn = conn;
        this.membershipTable = membershipTable;
        this.peerID = peerID;
        this.sending = sending;
        this.executor = Executors.newSingleThreadExecutor();
        this.bq = new CS601BlockingQueue<>(1);
        this.brokerID = brokerID;
        this.connMap = connMap;
        this.inElection = inElection;
    }

    //after sending out heartbeat msg -> expecting heartbeat response
    //if no hb response from leader, enter election and send initial election msg, wait for election response or decision
    @Override
    public void run() {
        Runnable add = () -> {
            while(listening) {
                try {
                    byte[] result = conn.receive();
                    if (result != null) {
                        bq.put(Resp.Response.parseFrom(result));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        executor.execute(add);
        while(listening) {
            Resp.Response f;
            int replyingBrokerId = -1;
            f = bq.poll(delay * retires);

            // if there's response within timeout
            if (f != null) {
                //check if f is heartbeat or election message
                System.out.println("type in listener: " + f.getType());
                if (f.getType().equals("heartbeat")) {
                    inElection = false;
                } else if (f.getType().equals("election")) {
                    inElection = true;
                } else {
                    System.out.println("wrong type");
                }

                if (!inElection) {// if its heartbeat response
                    replyingBrokerId = f.getSenderID();
                    System.out.println("receiving heartbeat msg from peer: " + replyingBrokerId);
                    membershipTable.getMemberInfo(replyingBrokerId).setAlive(true);

                    Resp.Response heartBeatMessage = Resp.Response.newBuilder().setType("heartbeat").setSenderID(brokerID).build();
                    conn.send(heartBeatMessage.toByteArray());
//                    try {
//                        Thread.sleep(500);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }

                }
                else if(inElection){// if its election response
                    int senderId = f.getSenderID();
                    int newLeader = f.getWinnerID();

                    if (newLeader == -1) {// if winner is -1 ... its a simple election response, still in election
                        System.out.println("In Election, receiving election response from the lower id broker " + senderId);
                        // me can stop election Bc there's someone more qualified than me to be the leader
                        System.out.println("now waiting for election decision from other broker");
                        inElection = true;
                        sending = false;

                    } else { // if winner is not -1 ... we have a winner
                        System.out.println("new leader id:" + newLeader);
                        // int oldLeader = membershipTable.getLeaderID();
                        int oldLeader = currentLeaderBeforeMarkDead;
                        System.out.println("old leader id:" + oldLeader);
                        if (oldLeader != -1) { // there's a leader
                            membershipTable.switchLeaderShip(oldLeader, newLeader);//update new leader
                        } else {
                            System.out.println("weird ... no current leader right now");
                        }

                 /*       //if this broker is leader, send table to load balancer
                        if(membershipTable.getMemberInfo(brokerID).isLeader){
                            //get new leader hostname and port
                            String peerHostName = Utilities.getHostnameByID(newLeader);
                            int peerPort = Utilities.getPortByID(newLeader);
                            //get LB hostname and port
                            String LBHostName = Utilities.getHostnameByID(0);
                            int LBPort = Utilities.getPortByID(0);
                            Connection connLB = new Connection(LBHostName, LBPort, true); // make connection to peers in config
                            Utilities.leaderConnectToLB(LBHostName, LBPort, peerHostName, peerPort, connLB);

                            //cancel old leader's leadership
                            Utilities.sendMembershipTableUpdates(connMap.get(0), "updateLeader", brokerID, oldLeader,
                                    "", 0, "", false, membershipTable.getMemberInfo(oldLeader).isAlive);
                            // set new leadership
                            Utilities.sendMembershipTableUpdates(connMap.get(0), "updateLeader", brokerID, newLeader,
                                    "", 0, "", true, membershipTable.getMemberInfo(newLeader).isAlive);
                            membershipTable.print();
                        }
*/
                        inElection = false; // election ended on my end
                        System.out.println("election ended");
                        Resp.Response heartBeatMessage = Resp.Response.newBuilder().setType("heartbeat").setSenderID(brokerID).build();
                        conn.send(heartBeatMessage.toByteArray());

                    }
                }

            } else { // if no response within timeout
                FailureDetector failureDetector = new FailureDetector(membershipTable, peerID, inElection, conn, brokerID, connMap, listening);
                failureDetector.run();
                inElection = failureDetector.getElectionStatus();
                currentLeaderBeforeMarkDead = failureDetector.getCurrentLeaderBeforeMarkDead();
            }
        }

        // else if within num of retires, send same heart beat again, go back to while loop

    }


}
