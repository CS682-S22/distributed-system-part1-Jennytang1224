import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.DataInfo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class Broker {
    private int port;
//    private String brokerHostName;
    private ServerSocket serverSocket;
    private Socket socket;
    private DataInputStream input;
    private DataOutputStream output;
    private volatile boolean running;
    private CopyOnWriteArrayList<byte[]> topics;
    private Map<String, CopyOnWriteArrayList> topicsMap;


    public Broker(int port) {
        this.port = port;
        this.topics = new CopyOnWriteArrayList<>();
        this.running = true;
        this.topicsMap = new HashMap<>();

        try {
            this.serverSocket = new ServerSocket(port);
            this.socket = this.serverSocket.accept();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //broker receive data from producer
    public byte[] receive()  {
        byte[] buffer = null;
        try {
            int length = input.readInt();
            if(length > 0) {
                buffer = new byte[length];
                input.readFully(buffer, 0, buffer.length);
            }
        } catch (EOFException ignored) {} //No more content available to read
        catch (IOException exception) {

            System.err.printf(" Fail to receive message ");
        }
        return buffer;
    }

    // write received record in bytes to the list
    public void writeToCluster(){
        DataInfo.Data d = null;
        byte[] recordBytes = receive();
        if (recordBytes == null || recordBytes.length == 0 ) {
            System.out.println("nothing received");
        }
        else {
            try {
                d = DataInfo.Data.parseFrom(recordBytes);
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
            String topic = d.getTopic();
            if(this.running) {
                if(this.topicsMap.containsKey(topic)){ //if key is in map
                    topicsMap.get(topic).add(recordBytes);
                }
                else{ //if key is not in the map, create CopyOnWriteArrayList and add first record
                    CopyOnWriteArrayList newList = new CopyOnWriteArrayList<>();
                    newList.add(recordBytes);
                    topicsMap.put(topic, newList);
                }
            }
        }
    }

    // read from broker 1 with specific topic
    public byte[] readFromCluster(String topic){
        if(this.running) {
            //in the hashmap, get the corresponding list of this topic
            CopyOnWriteArrayList<byte[]> topicList = topicsMap.get(topic);

            // start getting the all record from this topic
            for (int i = 0; i < topicList.size(); i++) {
                byte[] pulledSingleRecord = this.topics.get(i);
                // send ALL record in this list to the consumer
                //send()...
            }
        }
        //notify other brokers to send their data related to this topic to the consumer
        notifyOtherBrokers();
        return new byte[0];
    }

    // broker1 will notifty all other brokers to send this topic to the consumer
    public void notifyOtherBrokers(){

    }

    public synchronized void shutdown() {
        this.running = false;
    }
}
