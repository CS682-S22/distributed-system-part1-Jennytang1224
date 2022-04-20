import com.google.protobuf.InvalidProtocolBufferException;
import dsd.pubsub.protos.MessageInfo;

import java.io.*;
import java.net.Socket;
import java.time.Duration;

public class RunLeaderBasedConsumer {

    public static void main(String[] args) throws IOException {
        //usage: brokerLocation topic startingPosition
        if(!Utilities.validateArgsConsumer(args)){
            System.exit(-1);
        }
        String LBLocation = args[0];
        String topic = args[1];
        int startingPosition = Integer.parseInt(args[2]);

        // get leader location
        LeaderBasedConsumer leaderBasedConsumer = new LeaderBasedConsumer(LBLocation, topic, startingPosition);
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //send request directly to broker
        String leadBrokerLocation = leaderBasedConsumer.getLeadBrokerLocation();
        LeaderBasedConsumer leaderBasedConsumerToBroker = new LeaderBasedConsumer(leadBrokerLocation, topic, startingPosition);


        //running application thread for pulling
        RunLeaderBasedApplication app = new RunLeaderBasedApplication(LBLocation, topic, startingPosition, leaderBasedConsumerToBroker);
        Thread runApp = new Thread(app);
        runApp.start();

        int trackSize = -1;
        int lastSize = 0;
        while(true) {
            int sizeSavedToBq = leaderBasedConsumerToBroker.getPositionCounter();
            if(sizeSavedToBq != trackSize) {
                startingPosition += sizeSavedToBq;
                startingPosition -= lastSize;
                lastSize = sizeSavedToBq;
            }
            else{
                startingPosition += 0;
            }
            leaderBasedConsumerToBroker.subscribe(topic, startingPosition);

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