
import dsd.pubsub.protos.Resp;
import java.util.HashMap;
import java.util.TimerTask;



class HeartBeatSender extends TimerTask implements Runnable {
    private String name;
    private String port;
    private Connection conn;
    boolean sending = true;
    int brokerID;

    private String peerHostName;
    private int peerPort;
    private int peerID;
    private HashMap<Integer, Connection> connMap;
    MembershipTable membershipTable;

    public HeartBeatSender(String name, String port, Connection conn, String peerHostName, int peerPort, HashMap<Integer, Connection> connMap, MembershipTable membershipTable) {
        this.name = name;
        this.port = port;
        this.brokerID = Utilities.getBrokerIDFromFile(name, String.valueOf(port), "files/brokerConfig.json");
        this.conn = conn;
        this.peerHostName = peerHostName;
        this.peerPort = peerPort;
        this.peerID = Utilities.getBrokerIDFromFile(peerHostName, String.valueOf(peerPort), "files/brokerConfig.json");
        this.connMap = connMap;
        this.membershipTable = membershipTable;
    }

    @Override
    public void run() {
        while (sending) {
            //send heartbeat msg
            Resp.Response heartBeatMessage = Resp.Response.newBuilder().setType("heartbeat").setSenderID(brokerID).build();
            conn.send(heartBeatMessage.toByteArray());

            //start listening for response
            HeartBeatListener heartBeatlistener = new HeartBeatListener(conn, membershipTable, peerID, sending, brokerID, connMap);
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

