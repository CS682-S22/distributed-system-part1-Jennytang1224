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
        String LBLocation = "Jennys-MacBook-Pro.local:1430";
        String filepath = "files/100_records.log";

//        //get lead broker location from LB
//        Socket socket = null;
//        Connection connectionWithLB = null;
//        String LBHostName = LBLocation.split(":")[0];
//        int LBPort = Integer.parseInt(LBLocation.split(":")[1]);
//        try {
//            socket = new Socket(LBHostName, LBPort);
//            connectionWithLB = new Connection(socket);
//            System.out.println("producer connect to load balancer for lead broker address");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        String peerHostName = Utilities.getHostName();
//        int peerPort = 1412;
//        PeerInfo.Peer peerInfo = PeerInfo.Peer.newBuilder()
//                .setType("producer")
//                .setHostName(peerHostName)
//                .setPortNumber(peerPort)
//                .build();
//        connectionWithLB.send(peerInfo.toByteArray());
//
//        System.out.println("producer sends first msg to LB with its identity...\n");
//



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

                        // producer send record to broker
                        if(count == 0) { //send initial message regardless
                            leaderBasedProducerToBroker.send(record.toByteArray());
                            count++;
                        }

                        try { // CHECK ACK within timeout
                            Thread.sleep(2000); // drop this num will cause not receiving ack on time
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        receivedAck = leaderBasedProducerToBroker.getAckStatus();
                        System.out.println("received ack: " + receivedAck);

                        if (!receivedAck) {
                            System.out.println("DID NOT RECEIVE ACK FROM LEAD BROKER...");
                            break;
                        } else {
                            leaderBasedProducerToBroker.send(record.toByteArray());
                            System.out.println("An ACK received and a message has been send!");
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