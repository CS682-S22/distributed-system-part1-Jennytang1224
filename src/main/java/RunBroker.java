import java.io.IOException;
import java.util.List;

public class RunBroker {
    public static void main(String[] args){
        List<Object> maps = Utilities.readConfig();
        System.out.println(maps.size());
        IPMap ipMap = (IPMap) maps.get(0);
        System.out.println(ipMap.toString());
        PortMap portMap = (PortMap) maps.get(1);
        System.out.println(portMap.toString());
        String brokerHostName = Utilities.getHostName();
        System.out.println(brokerHostName);
        System.out.println(ipMap.getIdByIP(brokerHostName));
        System.out.println(portMap.getPortById(ipMap.getIdByIP(brokerHostName)));
        int brokerPort = Integer.parseInt(portMap.getPortById(ipMap.getIdByIP(brokerHostName)));
        Broker broker = new Broker(brokerHostName, brokerPort);
        try {
            broker.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
