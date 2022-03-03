import java.time.Duration;
import java.util.List;

public class Consumer {
    private String brokerLocation;
    private String topic;
    private int startingPosition;

    public Consumer(String brokerLocation, String topic, int startingPosition) {
        this.brokerLocation = brokerLocation;
        this.topic = topic;
        this.startingPosition = startingPosition;
    }

    public byte[] poll(Duration timeout){
        return new byte[0];
    }

    public void subscribe(List<String> topics){

    }

    public void close(){

    }
}
