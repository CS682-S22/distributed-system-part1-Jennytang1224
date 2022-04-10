
import dsd.pubsub.protos.Resp;
import dsd.pubsub.protos.Response;

import javax.sound.sampled.LineListener;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;


class HeartBeatSender extends TimerTask implements Runnable {
    private String name;
    private String port;
    private Connection conn;
    volatile boolean sending = true;
    int brokerID;
    private String peerHostName;
    private int peerPort;
    private int peerID;
    private HashMap<Integer, Connection> connMap;
    MembershipTable membershipTable;
    volatile boolean inElection = false;
    private CS601BlockingQueue<Resp.Response> bq;
    Resp.Response f;


    private ExecutorService executor;
    int delay = 2000;
    int retires = 3;
//    int peerID;
//    volatile boolean sending;
//    volatile boolean inElection;
//    int brokerID;
//    private HashMap<Integer, Connection> connMap;
    int currentLeaderBeforeMarkDead;
    volatile boolean electionStatus;
    boolean listening = true;


    public HeartBeatSender(String name, String port, Connection conn, String peerHostName, int peerPort,
                           HashMap<Integer, Connection> connMap, MembershipTable membershipTable, boolean inElection) {
        this.name = name;
        this.port = port;
        this.brokerID = Utilities.getBrokerIDFromFile(name, String.valueOf(port), "files/brokerConfig.json");
        this.conn = conn;
        this.peerHostName = peerHostName;
        this.peerPort = peerPort;
        this.peerID = Utilities.getBrokerIDFromFile(peerHostName, String.valueOf(peerPort), "files/brokerConfig.json");
        this.connMap = connMap;
        this.membershipTable = membershipTable;
        this.inElection = inElection;
    }

    @Override
    public void run() {
        Resp.Response heartBeatMessage = Resp.Response.newBuilder().setType("heartbeat").setSenderID(brokerID).build();
        conn.send(heartBeatMessage.toByteArray());
        //start listening for response
        HeartBeatListener heartBeatlistener = new HeartBeatListener(conn, membershipTable, peerID, sending, brokerID, connMap, inElection);
        Thread th = new Thread(heartBeatlistener);
        th.start();
//        inElection = heartBeatlistener.getElectionStatus();
//        System.out.println("******Election: " + inElection);

/*
        Resp.Response heartBeatMessage = Resp.Response.newBuilder().setType("heartbeat").setSenderID(brokerID).build();
        conn.send(heartBeatMessage.toByteArray());

        //after sending out heartbeat msg -> expecting heartbeat response
        //if no hb response from leader, enter election and send initial election msg, wait for election response or decision

        while(listening) {

//            Resp.Response f;
//            Runnable add = () -> {
//                try {
//                    byte[] result = conn.receive();
//                    if (result != null) {
//                        bq.put(Resp.Response.parseFrom(result));
//                    }
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            };
//
//            executor.execute(add);
//            int replyingBrokerId = -1;

            int replyingBrokerId = -1;

            f = bq.poll(delay * retires);
            // if there's response within timeout
            if (f != null) {
                //check if f is heartbeat or election message
                System.out.println("type in listener: " + f.getType());
                if (f.getType().equals("heartbeat")) {
                    // inElection = false;
                } else if (f.getType().equals("election")) {
                    inElection = true;
                } else {
                    System.out.println("wrong type");
                }

                if (!inElection) {// if its heartbeat response
                    //  heartBeat = f.getHeartBeat();
                    replyingBrokerId = f.getSenderID();
                    System.out.println("receiving heartbeat msg from peer: " + replyingBrokerId);
//                    Resp.Response heartBeatMessage = Resp.Response.newBuilder().setType("heartbeat").setSenderID(brokerID).build();
//                    conn.send(heartBeatMessage.toByteArray());

                } else {// if its election response
                    int senderId = f.getSenderID();
                    int newLeader = f.getWinnerID();

                    if (newLeader == -1) {// if winner is -1 ... its a simple election response, still in election
                        System.out.println("In Election, receiving election response from the lower id broker " + senderId);
                        // me can stop election Bc there's someone more qualified than me to be the leader
                        System.out.println("now waiting for election decision from other broker");
                        electionStatus = true;

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

                        try {
                            Thread.sleep(3000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        electionStatus = false; // election ended on my end
                        System.out.println("election ended");
                    }
                }

            } else { // if no response within timeout
                FailureDetector failureDetector = new FailureDetector(membershipTable, peerID, inElection, conn, brokerID, connMap);
                failureDetector.run();
                // electionStatus = failureDetector.getElectionStatus();
                //  setInElection(electionStatus);
                inElection = failureDetector.getElectionStatus();
                // System.out.println("election after FD: " + electionStatus);
                System.out.println("election after FD: " + inElection);
                currentLeaderBeforeMarkDead = failureDetector.getCurrentLeaderBeforeMarkDead();
                //stop the connection since the peer is dead
                sending = false;
            }
        }

        // else if within num of retires, send same heart beat again, go back to while loop
*/

  /*      while (sending) {

            System.out.println("inside sending, election: " + inElection);

            if(!inElection) { // if in election, do not send heartbeat
                //send heartbeat msg
                Resp.Response heartBeatMessage = Resp.Response.newBuilder().setType("heartbeat").setSenderID(brokerID).build();
                conn.send(heartBeatMessage.toByteArray());
            }


            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            sending = heartBeatlistener.getSending();
            System.out.println("******Sending: " + sending);
//            inElection = heartBeatlistener.getElectionStatus();
//            System.out.println("******Election: " + inElection);

        }
*/




    }




    public boolean getElectionStatus(){
        return inElection;
    }



}

