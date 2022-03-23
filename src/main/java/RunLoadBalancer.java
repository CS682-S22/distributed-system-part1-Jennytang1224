import java.io.IOException;
import java.util.List;

public class RunLoadBalancer {
    public static void main(String[] args){
        // 3 5 (3 broker and 5 partitions) brokerConfig
        if(args.length == 0){
            System.out.println("enter number of broker and partition");
            return;
        }
        else if (args.length < 3){
            System.out.println("missing another argument");
            return;
        }
        else if (args.length > 3){
            System.out.println("invalid number of arguments");
            return;
        }

        int numOfBrokers = Integer.parseInt(args[0]);
        int numOfPartitions = Integer.parseInt(args[1]);
        String brokerConfigFile = args[2];

        List<Object> maps = Utilities.readConfig();
        IPMap ipMap = (IPMap) maps.get(0);
        PortMap portMap = (PortMap) maps.get(1);
        String LBHostName = Utilities.getHostName();
        int LBPort = Integer.parseInt(portMap.getPortById(ipMap.getIdByIP(LBHostName)));
        LoadBalancer loadBalancer = new LoadBalancer(LBHostName, LBPort, numOfBrokers, numOfPartitions, brokerConfigFile);

        try {
            loadBalancer.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
