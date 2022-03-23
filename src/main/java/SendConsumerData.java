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
    HashMap<String, HashMap<Integer, CopyOnWriteArrayList<byte[]>>> topicMap;
    int startingPosition;
    String topic;
    HashMap<Integer, Connection> connMap;
    List<HashMap<String, HashMap<Integer, CopyOnWriteArrayList<byte[]>>>> topicMapList;
    int brokerID;


    public SendConsumerData(Connection consumerConnection, byte[] recordBytes, List<HashMap<String, HashMap<Integer,
            CopyOnWriteArrayList<byte[]>>>> topicMapList, HashMap<Integer, Connection> connMap, int brokerID){
        this.consumerConnection = consumerConnection;
        this.recordBytes = recordBytes;
        this.connMap = connMap;
        this.topicMapList = topicMapList;
        this.brokerID = brokerID;

    }

    @Override
    public void run() {
        // get correct topicMap by brokerID
        topicMap = topicMapList.get(brokerID-1);
        MessageInfo.Message d = null;
        try {
            d = MessageInfo.Message.parseFrom(recordBytes);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        topic = d.getTopic();
        startingPosition = d.getOffset();
        System.out.println("Broker: " + brokerID + " -> Consumer subscribed to: " + topic + ", at position: " + startingPosition);

        System.out.println("consumer topic map: " + topicMap);
        if (!topicMap.containsKey(topic)) {
            System.out.println("No topic called '" + topic + "' in this broker!");
        } else {
            partitionMap = topicMap.get(topic);
            System.out.println("there are " + partitionMap.size() + " partitions in this consumer with topic: " + topic);

            for (Map.Entry<Integer, CopyOnWriteArrayList<byte[]>> entry : partitionMap.entrySet()) {
                topicList = entry.getValue();
                // start getting the all record from this topic from starting position
                for (int j = 0; j < topicList.size(); j++) {
                    byte[] record = topicList.get(j);
                    int id = -1;
                    try {
                        id = MessageInfo.Message.parseFrom(record).getOffset();
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }
                    if((id > 0) && (id >= startingPosition)) {
                        consumerConnection.send(record);
                        System.out.println("New data in partition: " + entry.getKey() +" - A record has been sent to the consumer \n");
                    }
                    System.out.println("no new data in partition: " + entry.getKey() + "\n");
                }
            }
        }
    }
}
