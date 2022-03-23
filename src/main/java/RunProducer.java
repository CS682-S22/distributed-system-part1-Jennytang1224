import com.google.protobuf.ByteString;
import dsd.pubsub.protos.MessageInfo;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * run producer
 */
public class RunProducer {
    public static void main(String[] args){
        //usage: LBLocation filepath
        if(!Utilities.validateArgsProducer(args)){
            System.exit(-1);
        }
        String brokerLocation = args[0];
        String filepath = args[1];
        // Open a connection to the Broker by creating a new Producer object
        // send producer identity to broker
        Producer producer = new Producer(brokerLocation);
        // for each data record, send topic, key and data
        ByteString data = null;
        String topic;
        String key;
        int partition = 0;
        int offset;
        try{
            FileInputStream fstream = new FileInputStream(filepath);
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
            String strLine;
            /* read log line by line */
            while ((strLine = br.readLine()) != null)   {
                if (strLine.contains("GET /image/") || strLine.contains("GET /product/")) {
                    data = ByteString.copyFromUtf8(strLine);
                    System.out.println(strLine);
                    Pattern pattern =  Pattern.compile("GET /(.+?)/(.+?)/");
                    Matcher m =  pattern.matcher(strLine);
                    m.find();
                    topic = m.group(1);
                    key = m.group(2);
                    System.out.println(topic);
                    System.out.println(key);
                    if (key.length() < 10) { // sanity check
                        // build protobuffer
                        offset = data.size(); // increase by offset
                        System.out.println("set offset: " + offset);
                        MessageInfo.Message record = MessageInfo.Message.newBuilder()
                                .setTopic(topic)
                                .setKey(key)
                                .setValue(data)
                                .setPartition(partition)
                                .setOffset(offset)
                                .build();
                        // producer send record to broker
                        producer.send(record.toByteArray());
                        System.out.println("message has been send!");
                    }
                }
            }
            fstream.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}
