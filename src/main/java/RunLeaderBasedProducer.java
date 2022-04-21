import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import dsd.pubsub.protos.MessageInfo;
import dsd.pubsub.protos.PeerInfo;

import javax.swing.plaf.synth.SynthOptionPaneUI;
import java.io.*;
import java.net.Socket;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RunLeaderBasedProducer {
    public static void main(String[] args){
        //usage: brokerLocation filepath
        if(!Utilities.validateArgsProducer(args)){
            System.exit(-1);
        }

        String LBLocation = args[0];
        String filepath  = args[1];

        boolean receivedAck;
        LeaderBasedProducer leaderBasedProducer = new LeaderBasedProducer(LBLocation);
        try {
            Thread.sleep(1500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        String leadBrokerLocation = leaderBasedProducer.getLeadBrokerLocation();
        LeaderBasedProducer leaderBasedProducerToBroker = new LeaderBasedProducer(leadBrokerLocation);

        // for each data record, send topic, key and data
        ByteString data = null;
        String topic;
        String key;
        int partition = 0;
        int offset;
        int count = 0;
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
                        MessageInfo.Message record = MessageInfo.Message.newBuilder()
                                .setTopic(topic)
                                .setKey(key)
                                .setValue(data)
                                .setPartition(partition)
                                .setOffset(offset)
                                .build();

                        try { // CHECK ACK within timeout
                            Thread.sleep(2500); // drop this num will cause not receiving ack on time
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        if(count == 0) { // first time will send data regardless
                            receivedAck = true;
                            count++;
                        }else {
                            receivedAck = leaderBasedProducerToBroker.getAckStatus();
                            System.out.println("received ack: " + receivedAck);
                        }

                        if (!receivedAck) {
                            System.out.println("DID NOT RECEIVE ACK FROM LEAD BROKER...");
                            break;
                        } else {
                            leaderBasedProducerToBroker.send(record.toByteArray());
                            System.out.println("\nAn ACK received and a message has been send!\n");
                        }
                    }
                }
            }
            fstream.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}