import java.io.IOException;
import java.util.List;

/**
 * run load balancer
 */
public class RunLeaderBasedLoadBalancer {
    public static void main(String[] args){
        List<Object> maps = Utilities.readBrokerConfig();
        IPMap ipMap = (IPMap) maps.get(0);
        PortMap portMap = (PortMap) maps.get(1);
        String LBHostName = Utilities.getHostName();
        int LBPort = Integer.parseInt(portMap.getPortById(ipMap.getIdByIP(LBHostName)));
//        String LBHostName = "Jennys-MacBook-Pro.local";
//        int LBPort = 1430;
        LeaderBasedLoadBalancer loadBalancer = new LeaderBasedLoadBalancer(LBHostName, LBPort);

        try {
            loadBalancer.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
