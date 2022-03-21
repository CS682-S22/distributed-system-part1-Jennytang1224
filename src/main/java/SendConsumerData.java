import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.MessageInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class SendConsumerData implements Runnable{
    Connection consumerConnection;
    byte[] recordBytes;
    static CopyOnWriteArrayList<byte[]> topicList;
    static HashMap<Integer, CopyOnWriteArrayList<byte[]>> partitionMap;
    HashMap<String, HashMap<Integer, CopyOnWriteArrayList<byte[]>>> topicMap;// <topic1: topic1_list, topic2: topic2_list>
    int startingPosition;
    String topic;
    HashMap<Integer, Connection> connMap;

    public SendConsumerData(Connection consumerConnection, byte[] recordBytes, HashMap<String, HashMap<Integer,
            CopyOnWriteArrayList<byte[]>>> topicMap, HashMap<Integer, Connection> connMap){
        this.consumerConnection = consumerConnection;
        this.recordBytes = recordBytes;
        this.topicMap = topicMap;
        this.connMap = connMap;
    }

    @Override
    public void run() {
  //  while(true) {
      //  System.out.println("size of passed in topic map: " + this.topicMap.size());
        MessageInfo.Message d = null;
        try {
            d = MessageInfo.Message.parseFrom(recordBytes);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        topic = d.getTopic();
        startingPosition = d.getOffset();
        System.out.println("Consumer subscribed to: " + topic + ", at position: " + startingPosition);

        //in the hashmap, get the corresponding list of this topic
        System.out.println("consumer topic map size: " + topicMap);
        if (!topicMap.containsKey(topic)) {
            System.out.println("No topic called '" + topic + "' in this broker!");
        } else {
            partitionMap = topicMap.get(topic);
            System.out.println("consumer partition map size with topic: " + topic + ": " + partitionMap.size());

            for (Map.Entry<Integer, CopyOnWriteArrayList<byte[]>> entry : partitionMap.entrySet()) {
                topicList = entry.getValue();
                if (startingPosition >= topicList.size()) {
                    System.out.println("No new Data in partition: " + entry.getKey());
                } else {
                    // start getting the all record from this topic from starting position
                    for (int i = startingPosition; i < topicList.size(); i++) {
                        byte[] singleRecord = topicList.get(i);
                        // send ALL record in this list to the consumer
                        consumerConnection.send(singleRecord);
                        System.out.println("A record has sent to the consumer");
                    }
                }
            }
        }
  //      }
    }
}
