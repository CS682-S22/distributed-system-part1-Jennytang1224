import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RunConsumer {
    public static void main(String[] args) throws IOException {
        //usage: brokerLocation topic startingPosition
     //   check argument length
        if(args.length == 0){
            System.out.println("enter topic");
            return;
        }
        else if (args.length > 2){
            System.out.println("invalid number of arguments");
            return;
        }
//         Specify the location of the broker, topic of interest for this specific
//         consumer object, and a starting position in the message stream.


        String topic = args[0];
        int startingPosition = Integer.parseInt(args[1]);

//          String brokerLocation = "localhost:1431";
//          int startingPosition = 0;
//          String topic = "image";

        //based on info map, find the partition with the starting position
//        List<Integer> result = Utilities.readInfoMap(topic, startingPosition);
//        int brokerID = result.get(0);
//        int partitionID = result.get(1);
        // use brokerID to get connection and use connection

        List<Object> maps = Utilities.readBrokerConfig();
        IPMap ipMap = (IPMap) maps.get(0);
        PortMap portMap = (PortMap) maps.get(1);
        Consumer consumer = null;
        int requestCounter = 0;

        while(true) {

            for (int i = 1; i <= Utilities.numOfBrokersInSys; i++){ // loop though num of brokers
                // Connect to the consumer
                String brokerHostName = ipMap.getIpById(String.valueOf(i));
                int brokerPort =  Integer.parseInt(portMap.getPortById(String.valueOf(i)));
                String brokerLocation = brokerHostName + ":" + brokerPort;
                System.out.println("brokerlocation: " + brokerLocation);
    //        String brokerLocation = "Jennys-MacBook-Pro.local:1420" ;
                consumer = new Consumer(brokerLocation, topic, startingPosition);
             //   int trackSize = -1;
             //   int lastSize = 0;
//                int sizeSavedToBq = consumer.getPositionCounter();
//                System.out.println("sizeSavedToBq: " + sizeSavedToBq);
//                if(sizeSavedToBq != trackSize) {
//                    startingPosition += sizeSavedToBq;
//                    startingPosition -= lastSize;
//                    lastSize = sizeSavedToBq;
//                }
//                else{
//                    startingPosition += 0;
//                }

                System.out.println("starting position: " + startingPosition);
                consumer.subscribe(topic, startingPosition);
                try { // every 3 sec request new data
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            //    System.out.println("tracksize: " + trackSize);
             //   trackSize = sizeSavedToBq;
                System.out.println("\n");
            }
            requestCounter++;
            if(requestCounter != 0) { // not first time
                startingPosition = consumer.getMaxPosition() + 1;
            } // else if first time, will use input starting postion

        }



//
//        int offset = Utilities.getBytesOffsetById(startingPosition, Utilities.offsetFilePath);
//        System.out.println("offset: " + offset);
//        if(offset == -1){ // cant find the id
//            System.out.println("No such starting position exists, try again");
//            System.exit(-1);
//        }





        // Continue to pull messages...forever
//        while(true) {
//        //    byte[] message = consumer.poll(Duration.ofMillis(100));
//            // do something with this data!
//
//        }

        // When forever finally finishes...
      //  consumer.close();
    }
}
