import java.io.IOException;
import java.util.List;

public class RunLeaderBasedBroker {
    public static void main(String[] args) {
        //usage: brokerLocation filepath
        if(!Utilities.validateArgsBroker(args)){
            System.exit(-1);
        }
        boolean synchronous = Boolean.parseBoolean(args[0]);
        boolean failure = Boolean.parseBoolean(args[1]);
        List<Object> maps = Utilities.readBrokerConfig();
        IPMap ipMap = (IPMap) maps.get(0);
        PortMap portMap = (PortMap) maps.get(1);

        String brokerHostName = Utilities.getHostName();
        int brokerPort = Integer.parseInt(portMap.getPortById(ipMap.getIdByIP(brokerHostName)));
        int brokerDataPort = Integer.parseInt(portMap.getPortRepById(ipMap.getIdByIP(brokerHostName)));
//        String brokerHostName = "Jennys-MacBook-Pro.local";
//        int brokerPort = 1431;
//        int brokerDataPort = 1441;

        LeaderBasedBroker broker = new LeaderBasedBroker(brokerHostName, brokerPort, brokerDataPort, synchronous, failure);
        try {
            broker.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}