import dsd.pubsub.protos.Resp;
import java.util.HashMap;
import java.util.TimerTask;


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

    }

}

