import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.MessageInfo;

import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class LeaderBasedSendConsumerData implements Runnable{
    Connection connection;
    byte[] recordBytes;
    Map<String, CopyOnWriteArrayList> topicMap;// <topic1: topic1_list, topic2: topic2_list>
    int startingPosition;
    int newStartingPosition;

    public LeaderBasedSendConsumerData(Connection connection, byte[] recordBytes,  Map<String, CopyOnWriteArrayList> topicMap ){
        this.connection = connection;
        this.recordBytes = recordBytes;
        this.topicMap = topicMap;
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
        String topic = d.getTopic();
        startingPosition = d.getOffset();
        System.out.println("Project2.Consumer subscribed to: " + topic + ", at position: " + startingPosition);
        //offset to id
        //   int id = Utilities.getIdByOffset(startingPosition, Utilities.offsetFilePath);

        //in the hashmap, get the corresponding list of this topic
        if (!topicMap.containsKey(topic)) {
            System.out.println("No topic called '" + topic + "' in the broker!");
        }
        CopyOnWriteArrayList<byte[]> topicList = topicMap.get(topic);


        if(startingPosition >= topicList.size()){ //
            System.out.println("No new Data yet...");
        }
        else {
            // start getting the all record from this topic
            for (int i = startingPosition; i < topicList.size(); i++) {
                byte[] singleRecord = topicList.get(i);
                // send ALL record in this list to the consumer
                connection.send(singleRecord);
                System.out.println("A record has sent to the consumer");
            }
        }


        //   int newStartingPosition = startingPosition;

//            try {
//                Thread.sleep(3000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }

//
//            int size = startingPosition + (topicList.size() - startingPosition);
//            System.out.println("current starting position: " + startingPosition);
//            System.out.println("size: " + size);
//            newStartingPosition += size;
//            System.out.println("new position: " + newStartingPosition);
//
//            Project2.Consumer consumer = new Project2.Consumer(connection, startingPosition);
//            consumer.subscribe(topic, startingPosition);

        notifyOtherBrokers();
        //      }
        //notify other brokers to send their data related to this topic to the consumer


    }


    // broker1 will notify all other brokers to send this topic to the consumer
    public static void notifyOtherBrokers(){

    }
}