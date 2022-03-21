import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.MessageInfo;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class ReceiveProducerData implements Runnable{
    byte[] recordBytes;
    // topicMap = {image:<p1:list; p2:list>; product: <p1:list>}
    private HashMap<String, HashMap<Integer, CopyOnWriteArrayList<byte[]>>> topicMap;

    //    private String outputPath = "files/InfoMap";
//    private int messageCounter;
//    private int offsetInMem;
    static CopyOnWriteArrayList<byte[]> topicList;
    static HashMap<Integer, CopyOnWriteArrayList<byte[]>> partitionMap;

    public ReceiveProducerData(byte[] recordBytes, HashMap<String, HashMap<Integer, CopyOnWriteArrayList<byte[]>>> topicMap,
                               int messageCounter, int offsetInMem) {
        this.recordBytes = recordBytes;
        this.topicMap = topicMap;
//        this.messageCounter = messageCounter;
//        this.offsetInMem = offsetInMem;
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
        int partitionID = d.getPartition();
        System.out.println(partitionID);
        // save msgs to the maps based on topics and partitions:
        //   if(running) {

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

        //   }
       System.out.println("topic map: " + topicMap);

    }

    /**
     * write bytes to files
     */
    private static void writeBytesToFile(String fileOutput, byte[] buf)
            throws IOException {
        try (FileOutputStream fos = new FileOutputStream(fileOutput, true)) {
            System.out.println("Producer sends offset data to the disk...");
            fos.write(buf);
            fos.write(10); //newline
            fos.flush();
        }
        catch(IOException e){
            System.out.println("file writing error :(");
        }
    }


}
