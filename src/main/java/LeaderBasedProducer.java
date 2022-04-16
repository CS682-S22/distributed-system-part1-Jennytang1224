import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.Acknowledgment;
import dsd.pubsub.protos.PeerInfo;
import java.io.*;
import java.net.Socket;
import java.util.List;

//producer send data to broker
public class LeaderBasedProducer {
    private String brokerLocation;
    private int brokerPort;
    private String brokerHostName;
    private Socket socket;
    private DataInputStream input;
    private DataOutputStream output;
    private Connection connection;
    Server server;
    String peerHostName;
    int peerPort;
    static String leadBrokerLocation;
    Receiver newReceiver;
    static boolean receivedAck = false;

    public LeaderBasedProducer(String BrokerLocation) {
        this.brokerLocation = BrokerLocation;
        this.brokerHostName = BrokerLocation.split(":")[0];
        this.brokerPort = Integer.parseInt(BrokerLocation.split(":")[1]);
        this.socket = null;

        // draft peerinfo
        String type = "producer";
        List<Object> maps = Utilities.readConfig();
        IPMap ipMap = (IPMap) maps.get(0);
        PortMap portMap = (PortMap) maps.get(1);
        peerHostName = Utilities.getHostName();
        //peerPort = Integer.parseInt(portMap.getPortById(ipMap.getIdByIP(peerHostName)));
        peerPort = 1412;

        try {
            this.socket = new Socket(this.brokerHostName, this.brokerPort);
            this.connection = new Connection(this.socket);
            this.input = new DataInputStream(new BufferedInputStream(this.socket.getInputStream()));
            this.output = new DataOutputStream(new BufferedOutputStream(this.socket.getOutputStream()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("this producer is connecting to Host " + brokerLocation);
        PeerInfo.Peer peerInfo = PeerInfo.Peer.newBuilder()
                .setType(type)
                .setHostName(peerHostName)
                .setPortNumber(peerPort)
                .build();
        this.connection.send(peerInfo.toByteArray());
        System.out.println("producer sends peer info to broker ...\n");
        try { // every 3 sec request new data
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        newReceiver = new Receiver(peerHostName, peerPort, this.connection);
        Thread serverReceiver = new Thread(newReceiver);
        serverReceiver.start();

    }




    public String getLeadBrokerLocation(){
        return leadBrokerLocation;
    }

    public boolean getAckStatus(){
        System.out.println("inside getter: " + newReceiver.receivedAck);
        return  newReceiver.receivedAck;
    }


    static class Receiver implements Runnable {
        private String name;
        private int port;
        private Connection conn;
        boolean receiving = true;
        int counter = 0;
        String type;
        boolean receivedAck = false;

        public Receiver(String name, int port, Connection conn) {
            this.name = name;
            this.port = port;
            this.conn = conn;
        }


        @Override
        public void run() {
            Acknowledgment.ack response = null;
            while (receiving) {

                byte[] buffer = conn.receive();
                if (buffer == null || buffer.length == 0) {
                    //receivedAck = false;
                    // System.out.println("nothing received/ finished receiving");
                } else {// Receive from LB
                    try {
                        response = Acknowledgment.ack.parseFrom(buffer);
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }
                    if (response.getSenderType().equals("loadBalancer")) {//broker location
                        leadBrokerLocation = response.getLeadBrokerLocation();
                        System.out.println("received lead broker location from LB: " + leadBrokerLocation);
                    }
                    else if (response.getSenderType().equals("leadBrokerACK")) { // ack on data
                        System.out.println("received ack from lead broker!!!!");
                        receivedAck = true;
                    }
                    else if (response.getSenderType().equals("leadBrokerNOACK")) {
                        System.out.println("NO ack from lead broker!!!!");
                        receivedAck = false;
                    }
                }
            }
        }
    }

    /**
     * producer send
     *
     */
    public void send(byte[] record) {
        writeToSocket(record);
    }

    /**
     * write data to socket

     */
    public void writeToSocket(byte[] message){
        try {
            this.output.writeInt(message.length);
            this.output.write(message);
            this.output.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
