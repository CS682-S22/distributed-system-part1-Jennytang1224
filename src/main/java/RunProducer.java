import dsd.pubsub.protos.DataInfo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RunProducer {
    public static void main(String[] args){
        //usage: brokerLocation
        //check argument length
//
//        if(args.length == 0){
//            System.out.println("enter topic and message");
//            return;
//        }

//        else if (args.length < 3){
//            System.out.println("missing another argument");
//            return;
//        }
//        else if (args.length > 3){
//            System.out.println("invalid number of arguments");
//            return;
//        }
        // Set the data and topic
//        String topic = args[0];
//        String message = args[1];
//        byte[] data = message.getBytes(StandardCharsets.UTF_8);
//        String brokerLocation = args[2];
        String brokerLocation = "localhost:9092";

        // Open a connection to the Broker by creating a new Producer object
        Producer producer = new Producer(brokerLocation);
        Broker broker = new Broker(9092);

        // for each data record, send topic, key and data
        String data;
        String topic;
        String key;
        try{
            FileInputStream fstream = new FileInputStream("files/access.log");
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
            String strLine;
            /* read log line by line */
            while ((strLine = br.readLine()) != null)   {
                /* parse strLine to obtain what you want */
                if (strLine.contains("GET /image/") || strLine.contains("GET /product/")) {
                    data = strLine;
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
                        DataInfo.Data record = DataInfo.Data.newBuilder()
                                .setTopic(topic)
                                .setKey(key)
                                .setData(data).build();
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


        // Close the connection
        producer.close();

    }
}
