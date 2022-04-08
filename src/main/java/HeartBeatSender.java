import dsd.pubsub.protos.HeartBeatMessage;
import dsd.pubsub.protos.Response;

import java.io.IOException;
import java.util.HashMap;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


class HeartBeatSender extends TimerTask implements Runnable {
    private String name;
    private String port;
    private Connection conn;
    boolean sending = true;
    int brokerID;
    int retires = 3;
    private CS601BlockingQueue<HeartBeatMessage.HeartBeat> bq;
    private ExecutorService executor;
    private String peerHostName;
    private int peerPort;
    private int peerID;
    private HashMap<Integer, Connection> connMap;
    // int retryCount = 1;
    MembershipTable membershipTable;
    int delay = 1000;

    public HeartBeatSender(String name, String port, Connection conn, String peerHostName, int peerPort, HashMap<Integer, Connection> connMap, MembershipTable membershipTable) {
        this.name = name;
        this.port = port;
        this.brokerID = Utilities.getBrokerIDFromFile(name, String.valueOf(port), "files/brokerConfig.json");
        this.conn = conn;
        this.peerHostName = peerHostName;
        this.peerPort = peerPort;
        this.peerID = Utilities.getBrokerIDFromFile(peerHostName, String.valueOf(peerPort), "files/brokerConfig.json");
        this.bq = new CS601BlockingQueue<>(1);
        this.executor = Executors.newSingleThreadExecutor();
        this.connMap = connMap;
        this.membershipTable = membershipTable;
    }

    @Override
    public void run() {
        while (sending) {
            //send heartbeat msg
            Response.OneResponse heartBeatMessage = Response.OneResponse.newBuilder()
                    .setHeartBeat(Response.HeartBeat.newBuilder().setSenderID(brokerID).build()).build();
            conn.send(heartBeatMessage.toByteArray());



            //start listening for response
            HeartBeatListener heartBeatlistener = new HeartBeatListener(conn, membershipTable, peerID, sending);
            heartBeatlistener.run();

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            sending = heartBeatlistener.getSending();
        }
    }


}

