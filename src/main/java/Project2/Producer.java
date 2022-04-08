package Project2;

import dsd.pubsub.protos.PeerInfo;

import java.io.*;
import java.net.Socket;
import java.util.List;

//producer send data to broker
public class Producer {
    private String brokerLocation;
    private int brokerPort;
    private String brokerHostName;
    private Socket socket;
    private DataInputStream input;
    private DataOutputStream output;
    private Connection connection;


    public Producer(String brokerLocation) {
        this.brokerLocation = brokerLocation;
        this.brokerHostName =brokerLocation.split(":")[0];
        this.brokerPort = Integer.parseInt(brokerLocation.split(":")[1]);
        this.socket = null;
        try {
            this.socket = new Socket(this.brokerHostName, this.brokerPort);
            this.connection = new Connection(this.socket);
            this.input = new DataInputStream(new BufferedInputStream(this.socket.getInputStream()));
            this.output = new DataOutputStream(new BufferedOutputStream(this.socket.getOutputStream()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("this producer is connecting to broker " + brokerLocation);
        // draft peerinfo
        String type = "producer";
        List<Object> maps = Utilities.readConfig();
        IPMap ipMap = (IPMap) maps.get(0);
        PortMap portMap = (PortMap) maps.get(1);
        String peerHostName = Utilities.getHostName();
        int peerPort = Integer.parseInt(portMap.getPortById(ipMap.getIdByIP(peerHostName)));

        PeerInfo.Peer peerInfo = PeerInfo.Peer.newBuilder()
                .setType(type)
                .setHostName(peerHostName)
                .setPortNumber(peerPort)
                .build();
        this.connection.send(peerInfo.toByteArray());
        System.out.println("producer sends first msg to broker with its identity...\n");
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
