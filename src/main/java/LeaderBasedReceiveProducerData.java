import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.MessageInfo;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

// receive data and store them in topic map as {topic1: [..,..,..], topic2: [..,..,..,..,..,]}
public class LeaderBasedReceiveProducerData implements Runnable{
    Connection connection;
    ByteString recordBytes;
    private Map<String, CopyOnWriteArrayList<ByteString>> topicMap;// <topic1: topic1_list, topic2: topic2_list>
    private int messageCounter;
    boolean clear;

    public LeaderBasedReceiveProducerData(Connection connection, ByteString recordBytes,
                                          Map<String, CopyOnWriteArrayList<ByteString>> topicMap,
                                          int messageCounter, boolean clear) {
        this.connection = connection;
        this.recordBytes = recordBytes;
        this.topicMap = topicMap;
        this.messageCounter = messageCounter;
        this.clear = clear;
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

        if (topicMap.containsKey(topic)) { //if key is in map
            topicMap.get(topic).add(recordBytes);
        } else { //if key is not in the map, create CopyOnWriteArrayList and add first record
            CopyOnWriteArrayList<ByteString> newList = new CopyOnWriteArrayList<>();
            newList.add(recordBytes);
            topicMap.put(topic, newList);
        }
        messageCounter++;
        System.out.println(" >>>>>>>>>>>>> data stored and number of data received: " + messageCounter + "\n");
    }
}