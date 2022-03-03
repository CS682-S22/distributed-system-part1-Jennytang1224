import java.time.Duration;
import java.util.Arrays;

public class RunConsumer {
    public static void main(String[] args){
        //usage: topic brokerLocation startingPosition
        //check argument length
        if(args.length == 0){
            System.out.println("enter topic");
            return;
        }
        else if (args.length > 3){
            System.out.println("invalid number of arguments");
            return;
        }
        // Specify the location of the broker, topic of interest for this specific
        // consumer object, and a starting position in the message stream.
        String topic = args[0];
        String brokerLocation = args[1];
        int startingPosition = Integer.parseInt(args[2]);


        //  String brokerLocation = "localhost:9092";
        //  int startingPosition = 20;

        // Connect to the consumer
        Consumer consumer = new Consumer(brokerLocation, topic, startingPosition);

        consumer.subscribe(Arrays.asList(topic));
        System.out.println("subscribed to topic: " + topic);
        // Continue to pull messages...forever
        while(true) {
            byte[] message = consumer.poll(Duration.ofMillis(100));
            // do something with this data!

        }

        // When forever finally finishes...
      //  consumer.close();
    }
}
