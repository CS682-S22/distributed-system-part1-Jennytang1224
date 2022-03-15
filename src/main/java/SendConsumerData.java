import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.MessageInfo;

import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class SendConsumerData implements Runnable{
    Connection connection;
    byte[] recordBytes;
    static CopyOnWriteArrayList<byte[]> topicList;
    static Map<String, CopyOnWriteArrayList> topicMap;// <topic1: topic1_list, topic2: topic2_list>

    public SendConsumerData(Connection connection, byte[] recordBytes, CopyOnWriteArrayList<byte[]> topicList, Map<String, CopyOnWriteArrayList> topicMap ){
        this.connection = connection;
        this.recordBytes = recordBytes;
        this.topicList = topicList;
        this.topicMap = topicMap;
    }

    @Override
    public void run() {

        MessageInfo.Message d = null;
        try {
            d = MessageInfo.Message.parseFrom(recordBytes);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        String topic = d.getTopic();
        System.out.println("consumer subscribed to: " + topic);
        int startingPosition = d.getOffset();

        //in the hashmap, get the corresponding list of this topic
        if (!topicMap.containsKey(topic)) {
            System.out.println("No topic called '" + topic + "' in the broker!");
        }
        CopyOnWriteArrayList<byte[]> topicList = topicMap.get(topic);

        // start getting the all record from this topic
        for (int i = startingPosition; i < topicList.size(); i++) {
            byte[] singleRecord = topicList.get(i);
            // send ALL record in this list to the consumer
            connection.send(singleRecord);
            System.out.println("A record has sent to the consumer");
        }


        //notify other brokers to send their data related to this topic to the consumer
        notifyOtherBrokers();

    }


    // broker1 will notify all other brokers to send this topic to the consumer
    public static void notifyOtherBrokers(){

    }
}
