import java.io.IOException;

public class runBroker {
    public static void main(String[] args){
        Broker broker = new Broker("Jennys-MacBook-Pro.local", 1431); // hostname
        try {
            broker.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
