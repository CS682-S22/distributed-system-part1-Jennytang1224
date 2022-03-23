import java.io.IOException;
import java.util.List;

public class RunBroker {
    public static void main(String[] args){
//        List<Object> maps = Utilities.readConfig();
//        IPMap ipMap = (IPMap) maps.get(0);
//        PortMap portMap = (PortMap) maps.get(1);
//        String brokerHostName = Utilities.getHostName();
//        int brokerPort = Integer.parseInt(portMap.getPortById(ipMap.getIdByIP(brokerHostName)));
//        DistributedBroker broker = new DistributedBroker(brokerHostName, brokerPort);
//        try {
//            broker.run();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        if(args.length == 0){
            System.out.println("enter broker config file");
            return;
        }
        String brokerConfigFile = args[0];
        List<Object> maps = Utilities.readBrokerConfig(brokerConfigFile);
        IPMap ipMap = (IPMap) maps.get(0);
        PortMap portMap = (PortMap) maps.get(1);

//        String brokerHostName = "Jennys-MacBook-Pro.local";
//        int brokerPort = 1420;

        String brokerHostName = Utilities.getHostName();
        int brokerPort = Integer.parseInt(portMap.getPortById(ipMap.getIdByIP(brokerHostName)));
        DistributedBroker broker = new DistributedBroker(brokerHostName, brokerPort, brokerConfigFile);
        try {
            broker.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
