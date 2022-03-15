import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.MessageInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class ReceiveProducerData implements Runnable{
    Connection connection;
    byte[] recordBytes;
    private static CopyOnWriteArrayList<byte[]> topicList;
    private static Map<String, CopyOnWriteArrayList> topicMap;// <topic1: topic1_list, topic2: topic2_list>


    public ReceiveProducerData(Connection connection, byte[] recordBytes) {
        this.connection = connection;
        this.recordBytes = recordBytes;
        this.topicList = new CopyOnWriteArrayList<>();
        this.topicMap = new HashMap<>();

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
        //   }
        System.out.println("topic map size: " + topicMap.size());

    }
}
