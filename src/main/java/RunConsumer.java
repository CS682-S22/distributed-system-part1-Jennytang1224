import java.io.IOException;

public class RunConsumer {
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
//         Specify the location of the broker, topic of interest for this specific
//         consumer object, and a starting position in the message stream.

        String brokerLocation = args[0];
        String topic = args[1];
        int startingPosition = Integer.parseInt(args[2]);

//          String brokerLocation = "localhost:1431";
//          int startingPosition = 0;
//          String topic = "image";

        // Connect to the consumer

        Consumer consumer = new Consumer(brokerLocation, topic, startingPosition);


//        int offset = Utilities.getBytesOffsetById(startingPosition, Utilities.offsetFilePath);
//        System.out.println("offset: " + offset);
//        if(offset == -1){ // cant find the id
//            System.out.println("No such starting position exists, try again");
//            System.exit(-1);
//        }

        int trackSize = -1;
        while(true) {

            int sizeSavedToBq = consumer.getPositionCounter();
         //   System.out.println("sizeSavedToBq: " + sizeSavedToBq);
            if(sizeSavedToBq != trackSize) {
                startingPosition += sizeSavedToBq;
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
