import dsd.pubsub.protos.Acknowledgment;
import java.io.*;
import java.nio.charset.StandardCharsets;

import com.google.protobuf.InvalidProtocolBufferException;


public class RunLeaderBasedApplication implements Runnable {
    private String brokerLocation;
    private String topic;
    private int startingPosition;
    LeaderBasedConsumer consumer;


    public RunLeaderBasedApplication(String brokerLocation, String topic, int startingPosition, LeaderBasedConsumer consumer) {
        this.brokerLocation = brokerLocation;
        this.topic = topic;
        this.startingPosition = startingPosition;
        this.consumer = consumer;
    }

    @Override
    public void run(){
        while(true){
            String fileOutput = consumer.getOutputPath();
            byte[] m = null;
            Acknowledgment.ack d = null;

           //  application polls from bq
            try {
                m = consumer.poll(30);
            } catch (NullPointerException e){
            }
            if (m != null) { // received within timeout
                //save to file
                try {
                    d = Acknowledgment.ack.parseFrom(m);
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
                byte[] arr = d.getData().toString(StandardCharsets.ISO_8859_1).getBytes(StandardCharsets.ISO_8859_1);

                try {
                    Utilities.writeBytesToFile(fileOutput, arr);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
            else{
               // System.out.println("m == null");
            }
        }
    }

}
