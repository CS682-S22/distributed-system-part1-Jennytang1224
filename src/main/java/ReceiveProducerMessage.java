import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.MessageInfo;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class ReceiveProducerMessage implements Runnable{
    byte[] recordBytes;
    private String offsetOutputPath = "files/idMapOffset";
    private String infoOutputPath = "files/InfoMap";
    private int messageCounter;
    private int offsetInMem;
    int numOfBrokers;
    int numOfPartitions;
    Socket socket = null;
    private DataInputStream input;
    private DataOutputStream output;
    String brokerHostName;
    int brokerPort;
    HashMap<Integer, Connection> connMap;
    HashMap<String, Integer> counterMap;
    static HashMap<String, HashMap<Integer, CopyOnWriteArrayList<byte[]>>> topicMap;// <topic1: topic1_list, topic2: topic2_list>



    public ReceiveProducerMessage( byte[] recordBytes, int messageCounter,
                                   int offsetInMem, int numOfBrokers, int numOfPartitions, HashMap<Integer, Connection> connMap, HashMap<String, Integer> counterMap) {
        this.recordBytes = recordBytes;
        this.messageCounter = messageCounter;
        this.offsetInMem = offsetInMem;
        this.numOfBrokers = numOfBrokers;
        this.numOfPartitions = numOfPartitions;
        this.connMap = connMap;
        this.counterMap = counterMap;
        topicMap = new HashMap<>();
    }

    @Override
    public void run(){
        MessageInfo.Message d = null;
        int count;
        try {
            d = MessageInfo.Message.parseFrom(recordBytes);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        // create counterMap: continuous messageId per topic
        String topic = d.getTopic();
        if(counterMap.containsKey(topic)){
            count = counterMap.get(topic);
            count++;
            counterMap.put(topic, count);
        }else{
            count = 1;
            counterMap.put(topic, 1);
        }
        String key = d.getKey();
        ByteString data = d.getValue();
        int offset = d.getOffset();

        // calculate partitionID ba & brokerID sed on key
        int partitionID = Utilities.CalculatePartition(key, numOfPartitions);
        int brokerID = Utilities.CalculateBroker(partitionID, numOfBrokers);

        // send protobuf with partition ID via assigned broker connection
        MessageInfo.Message record = MessageInfo.Message.newBuilder()
                .setTopic(topic)
                .setKey(key)
                .setValue(data)
                .setPartition(partitionID)
                .setOffset(offset)
                .build();

        Connection connection = connMap.get(brokerID);
        connection.send(record.toByteArray());
        // save intermediate file:  msgID, key, topic, partitionID, BrokerID
        String line;
        line = count + "," + key + "," + topic + "," + partitionID + "," + brokerID;
        byte[] arr = line.getBytes(StandardCharsets.UTF_8);
        try {
            System.out.println(line);
            writeBytesToFile(infoOutputPath, arr);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Message has been sent to the assigned BROKER: " + brokerID + ", PARTITION: " + partitionID);


        // save intermediate data msg id, offset of bytes
        String line1;
        if(this.messageCounter == 0){
            line1 = this.messageCounter + "," + 0;
        } else {
            offsetInMem += d.getOffset();
            line1 = this.messageCounter + "," + offsetInMem;
        }
        this.messageCounter++;
        byte[] arr1 = line1.getBytes(StandardCharsets.UTF_8);
        try {
            writeBytesToFile(offsetOutputPath, arr1);
        } catch (IOException e) {
            e.printStackTrace();
        }




    }

    /**
     * write bytes to files
     */
    private static void writeBytesToFile(String fileOutput, byte[] buf)
            throws IOException {
        try (FileOutputStream fos = new FileOutputStream(fileOutput, true)) {
            System.out.println("data saved to info file");
            fos.write(buf);
            fos.write(10); //newline
            fos.flush();
        }
        catch(IOException e){
            System.out.println("file writing error :(");
        }
    }


}

