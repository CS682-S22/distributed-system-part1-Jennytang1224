import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.MessageInfo;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class ReceiveProducerData implements Runnable{
    Connection connection;
    byte[] recordBytes;
    private Map<String, CopyOnWriteArrayList> topicMap;// <topic1: topic1_list, topic2: topic2_list>
    private String outputPath = "idMapOffset";
    private int messageCounter;

    public ReceiveProducerData(Connection connection, byte[] recordBytes, Map<String, CopyOnWriteArrayList> topicMap, int messageCounter) {
        this.connection = connection;
        this.recordBytes = recordBytes;
        this.topicMap = topicMap;
        this.messageCounter = messageCounter;

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
        // save intermediate data msg id, offset of bytes
        String line;
        if(this.messageCounter == 0){
            line = this.messageCounter + "," + 0;
        }
        else {
            line = this.messageCounter + "," + d.getOffset();
        }
        this.messageCounter++;
        byte[] arr = line.getBytes(StandardCharsets.UTF_8);
        try {
            writeBytesToFile("files/" + outputPath, arr);
        } catch (IOException e) {
            e.printStackTrace();
        }


        //   }
        System.out.println("topic map size: " + topicMap.size());

    }

    /**
     * write bytes to files
     */
    private static void writeBytesToFile(String fileOutput, byte[] buf)
            throws IOException {
        try (FileOutputStream fos = new FileOutputStream(fileOutput, true)) {
            System.out.println("writing to the file...");
            fos.write(buf);
            fos.write(10); //newline
            fos.flush();
        }
        catch(IOException e){
            System.out.println("file writing error :(");
        }
    }


}
