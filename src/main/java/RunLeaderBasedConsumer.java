import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.MessageInfo;

import java.io.*;
import java.net.Socket;
import java.time.Duration;

public class RunLeaderBasedConsumer {

    public static void main(String[] args) throws IOException {
        //usage: brokerLocation topic startingPosition
        //   check argument length
        if(args.length == 0){
            System.out.println("enter topic");
            return;
        }
        else if (args.length > 3){
            System.out.println("invalid number of arguments");
            return;
        }

        String brokerLocation = args[0];
        String topic = args[1];
        int startingPosition = Integer.parseInt(args[2]);

        LeaderBasedConsumer consumer = new LeaderBasedConsumer(brokerLocation, topic, startingPosition);
        //running application thread for pulling
        RunLeaderBasedApplication app = new RunLeaderBasedApplication(brokerLocation, topic, startingPosition, consumer);
        Thread runApp = new Thread(app);
        runApp.start();

        int trackSize = -1;
        int lastSize = 0;
        while(true) {
            int sizeSavedToBq = consumer.getPositionCounter();
            if(sizeSavedToBq != trackSize) {
                startingPosition += sizeSavedToBq;
                startingPosition -= lastSize;
                lastSize = sizeSavedToBq;
            }
            else{
                startingPosition += 0;
            }
            consumer.subscribe(topic, startingPosition);

            try { // every 3 sec request new data
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            trackSize = sizeSavedToBq;
            System.out.println("\n");
        }
    }
}