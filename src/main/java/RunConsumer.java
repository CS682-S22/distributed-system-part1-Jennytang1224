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
        int start = 0;
        int receiveCounter = 0;
        int max = 0;

        while(true) {
            for (int i = 1; i <= Utilities.numOfBrokersInSys; i++){ // loop though num of brokers
                // Connect to the consumer
                System.out.println("Starting position: " + startingPosition);
                String brokerHostName = ipMap.getIpById(String.valueOf(i));
                int brokerPort =  Integer.parseInt(portMap.getPortById(String.valueOf(i)));
                String brokerLocation = brokerHostName + ":" + brokerPort;
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
                // everytime get to one broker, check max

                if(consumer.getMaxPosition() >= max){
                    max = consumer.getMaxPosition();
                }

                System.out.println(startingPosition);
                System.out.println(consumer.getReceiverCounter());
                if(requestCounter == 0) {
                    receiveCounter = startingPosition - 1 + consumer.getReceiverCounter();
                }else{
                    receiveCounter = startingPosition - 1;
                }
                System.out.println("max: " + max + ", receiverCounter: " + receiveCounter);
                if(max - start == receiveCounter){ // get through all brokers
                    if(requestCounter != 0) { // not first time
                        startingPosition = max + 1;
                    } // else if first time, will use input starting position
                }
                consumer.subscribe(topic, startingPosition);
            //    System.out.println("tracksize: " + trackSize);
             //   trackSize = sizeSavedToBq;

                try { // every 3 sec request new data
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            requestCounter++;
            //after iterating through num of brokers,
            // if max = counter -> increment starting position
         //   int max = consumer.getMaxPosition();
          //  int receiveCounter = consumer.getReceiverCounter();
            System.out.println("outside for loop: " + "max: " + max + ", receiverCounter: " + receiveCounter);
            if(max - start != receiveCounter){ // miss brokers -> no increment on starting position

            }else{
                if(requestCounter != 0) { // not first time
                    startingPosition = max + 1;
                } // else if first time, will use input starting position
            }
            // else -> keep same counter and go to for loop again

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
