import java.util.*;

/**
 * class create map that <host id -> ip>
 */
public class IPMap {
    private Map<String, String> ipMap;
    private String id;
    private String ipAddress;

    public IPMap(){
        this.ipMap = new HashMap<>();
    }

    /**
     * add to the map
     */
    public void put(String id, String ipAddress) {
        this.ipMap.put(id, ipAddress);
    }

    /**
     * get host id by given IP address
     */
    public String getIdByIP(String ip) {
        for (Map.Entry<String, String> entry : this.ipMap.entrySet()) {
            if (Objects.equals(ip, entry.getValue())) {
                return entry.getKey();
            }
        }
        return null;
    }

    /**
     * get ip address by id
     */
    public String getIpById(String id) {
        return this.ipMap.get(id);
    }

    /**
     * print map
     */
    public void printMap(){
        System.out.println(Collections.singletonList(this.ipMap));
    }

    /**
     * list all IP address in config file
     */
    public List<String> listAllIps(){
        List<String> lst = new ArrayList<>();
        for(Map.Entry<String, String> entry: this.ipMap.entrySet()) {
            lst.add(entry.getValue());
        }
        return lst;
    }
}
