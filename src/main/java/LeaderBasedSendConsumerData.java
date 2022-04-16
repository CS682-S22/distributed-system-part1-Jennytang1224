import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.Acknowledgment;
import dsd.pubsub.protos.BrokerToLoadBalancer;
import dsd.pubsub.protos.MessageInfo;

import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class LeaderBasedSendConsumerData implements Runnable{
    Connection connection;
    byte[] recordBytes;
    Map<String, CopyOnWriteArrayList<ByteString>> topicMap;// <topic1: topic1_list, topic2: topic2_list>
    int startingPosition;
    int newStartingPosition;

    public LeaderBasedSendConsumerData(Connection connection, byte[] recordBytes,  Map<String, CopyOnWriteArrayList<ByteString>> topicMap ){
        this.connection = connection;
        this.recordBytes = recordBytes;
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
        startingPosition = d.getOffset();
        System.out.println("Consumer subscribed to: " + topic + ", at position: " + startingPosition);

        //in the hashmap, get the corresponding list of this topic
        if (!topicMap.containsKey(topic)) {
            System.out.println("No topic called '" + topic + "' in the broker!");
        }
        CopyOnWriteArrayList<ByteString> topicList = topicMap.get(topic);


        if(startingPosition >= topicList.size()){ //
            System.out.println("No new Data yet...");
        }
        else {
            // start getting all record from this topic
            int count = 1;
            synchronized (this) {
                for (int i = startingPosition; i < topicList.size(); i++) {
                    ByteString singleRecord = topicList.get(i);
                    //   send ALL record in this list to the consumer
                    Acknowledgment.ack data = Acknowledgment.ack.newBuilder()
                            .setSenderType("leadBroker")
                            .setData(singleRecord)
                            .build();
                    connection.send(data.toByteArray());
                    // connection.send(singleRecord);
                    System.out.println("A record has been sent to the Consumer, count: " + count++);
                }
            }
        }

    }

}