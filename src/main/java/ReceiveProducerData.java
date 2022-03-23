import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.MessageInfo;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class ReceiveProducerData implements Runnable{
    byte[] recordBytes;
    private List<HashMap<String, HashMap<Integer, CopyOnWriteArrayList<byte[]>>>> topicMapList;
    static CopyOnWriteArrayList<byte[]> topicList;
    static HashMap<Integer, CopyOnWriteArrayList<byte[]>> partitionMap;
    int brokerID;
    HashMap<String, HashMap<Integer, CopyOnWriteArrayList<byte[]>>> topicMap;


    public ReceiveProducerData(byte[] recordBytes, List<HashMap<String, HashMap<Integer, CopyOnWriteArrayList<byte[]>>>> topicMapList, int brokerID) {
        this.recordBytes = recordBytes;
        this.topicMapList = topicMapList;
        this.brokerID = brokerID;

        if(topicMapList.size() == 0){
            for (int i = 0; i < 5; i++) {
                topicMapList.add(i, new HashMap<String, HashMap<Integer, CopyOnWriteArrayList<byte[]>>>());
            }
        }
    }

    @Override
    public void run(){
        // get correct topicMap by brokerID
        topicMap = topicMapList.get(brokerID - 1);
        MessageInfo.Message d = null;
        try {
            d = MessageInfo.Message.parseFrom(recordBytes);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        String topic = d.getTopic();
        int partitionID = d.getPartition();

        if(this.topicMap != null && this.topicMap.size() == 0) {
            topicList = new CopyOnWriteArrayList<>();
            partitionMap = new HashMap<>();
            topicList.add(recordBytes);
            partitionMap.put(partitionID, topicList);
            this.topicMap.put(topic, partitionMap);
            System.out.println(" ->>> saved first record");

        }else{ // if topic map is null
            if (this.topicMap.containsKey(topic)) { //if topic is in map
                partitionMap = topicMap.get(topic);
                if (partitionMap.containsKey(partitionID)) { // if partitionID is in topic, add
                    partitionMap.get(partitionID).add(recordBytes);
                } else { // if partitionID is not in topic, create a new inner map
                    topicList = new CopyOnWriteArrayList<>();
                    partitionMap.put(partitionID, topicList);
                    partitionMap.get(partitionID).add(recordBytes);
                }
            } else { //if topic is not in the map, create a new inner hashmap and add first record
                topicList = new CopyOnWriteArrayList<>();
                partitionMap = new HashMap<>();
                topicList.add(recordBytes);
                partitionMap.put(partitionID, topicList);
                this.topicMap.put(topic, partitionMap);
            }
            System.out.println(" -> saved to topicMap");
        }
     //  System.out.println("broker " + brokerID + ": topic map: " + topicMap);
    }

}
