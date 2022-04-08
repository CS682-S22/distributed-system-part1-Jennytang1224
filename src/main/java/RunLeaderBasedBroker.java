import java.io.IOException;
import java.util.List;

public class RunLeaderBasedBroker {
    public static void main(String[] args) {
        List<Object> maps = Utilities.readBrokerConfig("files/brokerConfig.json");
        IPMap ipMap = (IPMap) maps.get(0);
        PortMap portMap = (PortMap) maps.get(1);
     //   String brokerHostName = Utilities.getHostName();
     //   int brokerPort = Integer.parseInt(portMap.getPortById(ipMap.getIdByIP(brokerHostName)));
        String brokerHostName = "Jennys-MacBook-Pro.local";
        int brokerPort = 1431;

        LeaderBasedBroker broker = new LeaderBasedBroker(brokerHostName, brokerPort);
        try {
            broker.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}