package Project2;

import Project2.LoadBalancer;

import java.io.IOException;
import java.util.List;
/**
 * run load balancer
 */
public class RunLoadBalancer {
    public static void main(String[] args){
        // 3 5 (3 broker and 5 partitions) brokerConfig
        if(!Utilities.validateArgsLoadBalancer(args)){
            System.exit(-1);
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
