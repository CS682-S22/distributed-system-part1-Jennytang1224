package Project2;

import java.util.*;

/**
 * map host ip to port num
 */
public class PortMap {
    //map host ip address to port number
    private Map<String, String> portMap;
    private String id;
    private String portNumber;

    public PortMap(){
        this.portMap = new HashMap<>();
    }

    public void put(String id, String portNum) {
        this.portMap.put(id, portNum);
    }

    public String getPortById(String id) {
        return this.portMap.get(id);
    }

    public String getPortNumber() {
        return this.portNumber;
    }

    public void setPortNumber(String portNumber) {
        this.portNumber = portNumber;
    }

    public void printMap(){
        System.out.println(Collections.singletonList(this.portMap));
    }

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<Integer> listAllPorts(){
        List<Integer> lst = new ArrayList<>();
        for(Map.Entry<String, String> entry: this.portMap.entrySet()) {
            lst.add(Integer.parseInt(entry.getValue()));
        }
        return lst;
    }
}
