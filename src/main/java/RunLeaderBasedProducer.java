import com.google.protobuf.ByteString;
import dsd.pubsub.protos.MessageInfo;

import javax.sound.sampled.AudioFormat;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RunLeaderBasedProducer {
    public static void main(String[] args){
        //usage: brokerLocation filepath
        //check argument length
        if(args.length == 0){
            System.out.println("enter topic and message");
            return;
        }
        else if (args.length < 2){
            System.out.println("missing another argument");
            return;
        }
        else if (args.length > 2){
            System.out.println("invalid number of arguments");
            return;
        }

        String brokerLocation = args[0];
        String filepath = args[1];

//        String brokerLocation = "Jennys-MacBook-Pro.local:1431";
//        String filepath = "files/access_test.log";

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
                        offset = data.size();
                        System.out.println("set offset: " + offset);
                        // offset += 1; // monotonically increasing
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