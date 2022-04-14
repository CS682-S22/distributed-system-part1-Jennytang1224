import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.MessageInfo;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class LeaderBasedReceiveProducerMessage implements Runnable{
    private byte[] recordBytes;
    private String offsetOutputPath = Utilities.offsetFilePath;
    private String infoOutputPath = Utilities.InfoFileName;
    private int messageCounter;
    private HashMap<String, Integer> counterMap;
    private static HashMap<String, HashMap<Integer, CopyOnWriteArrayList<byte[]>>> topicMap;
    private Connection connWithLeadBroker;
    int currentLeadBroker;

    public LeaderBasedReceiveProducerMessage( byte[] recordBytes, int messageCounter, HashMap<String, Integer> counterMap, Connection connWithLeadBroker, int currentLeadBroker) {
        this.recordBytes = recordBytes;
        this.messageCounter = messageCounter;
        this.counterMap = counterMap;
        topicMap = new HashMap<>();
        this.connWithLeadBroker = connWithLeadBroker;
        this.currentLeadBroker = currentLeadBroker;

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

        // save intermediate file:  msgID, key, topic, partitionID, BrokerID
        String line;
        line = count + "," + key + "," + topic;
        byte[] arr = line.getBytes(StandardCharsets.UTF_8);
        try {
            System.out.println(line);
            writeBytesToFile(infoOutputPath, arr);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // send protobuf with partition ID via assigned broker connection
        MessageInfo.Message record = MessageInfo.Message.newBuilder()
                .setTopic(topic)
                .setKey(key)
                .setValue(data)
                .setOffset(count) // use msgid as offset
                .build();

        // send to broker
        System.out.println(connWithLeadBroker);
        connWithLeadBroker.send(record.toByteArray());
        System.out.println("Message has been sent to the lead BROKER: " + currentLeadBroker );
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
