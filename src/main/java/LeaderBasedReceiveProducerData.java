import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.MessageInfo;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

// receive data and store them in topic map as {topic1: [..,..,..], topic2: [..,..,..,..,..,]}
public class LeaderBasedReceiveProducerData implements Runnable{
    static Connection connection;
    byte[] recordBytes;
    private Map<String, CopyOnWriteArrayList> topicMap;// <topic1: topic1_list, topic2: topic2_list>
    private String outputPath = "files/idMapOffset";
    private int messageCounter;
    private int offsetInMem;

    public LeaderBasedReceiveProducerData(Connection connection, byte[] recordBytes, Map<String, CopyOnWriteArrayList> topicMap, int messageCounter, int offsetInMem) {
        this.connection = connection;
        this.recordBytes = recordBytes;
        this.topicMap = topicMap;
        this.messageCounter = messageCounter;
        this.offsetInMem = offsetInMem;
    }

    @Override
    public void run(){
        MessageInfo.Message d = null;
        try {
            d = MessageInfo.Message.parseFrom(recordBytes);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        String topic = d.getTopic();
        //   if(running) {
        if(topicMap.containsKey(topic)){ //if key is in map
            topicMap.get(topic).add(recordBytes);
        }
        else{ //if key is not in the map, create CopyOnWriteArrayList and add first record
            CopyOnWriteArrayList newList = new CopyOnWriteArrayList<>();
            newList.add(recordBytes);
            topicMap.put(topic, newList);
        }

        this.messageCounter++;
        System.out.println("number of messages received:" + messageCounter);
    }


}