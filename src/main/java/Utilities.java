import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.protobuf.ByteString;
import dsd.pubsub.protos.BrokerToLoadBalancer;
import dsd.pubsub.protos.PeerInfo;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


/**
 * A utility class for reading config files and getting ip and hostname
 */
public class Utilities {

    private static String HostConfigFileName = "files/config.json";
    static String InfoFileName = "files/InfoMap";
    static String offsetFilePath = "files/idMapOffset";
    private static String hostname;
    private static String brokerConfigFile = "files/brokerConfig.json";
    static int numOfBrokersInSys = 5;

    /**
     * get computer host name
     */
    public static String getHostName(){
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return hostname;
    }

    /**
     * Validates arguments passed in
     * @param args the array of arguments
     * @return true if arguments are valid, false otherwise
     */
    public static boolean validateArgsConsumer(String[] args) {
        //usage: topic startingPosition brokerConfig
        if(args.length == 0){
            System.out.println("enter topic");
            return false;
        }
        else if (args.length > 3){
            System.out.println("invalid number of arguments");
            return false;
        }
       return true;
    }


    /**
     * Validates arguments passed in
     * @param args the array of arguments
     * @return true if arguments are valid, false otherwise
     */
    public static boolean validateArgsProducer(String[] args) {
        //usage: LBLocation filepath
        if(args.length == 0){
            System.out.println("enter topic and message");
            return false;
        }
        else if (args.length < 2){
            System.out.println("missing another argument");
            return false;
        }
        else if (args.length > 2){
            System.out.println("invalid number of arguments");
            return false;
        }
        return true;
    }


    /**
     * Validates arguments passed in
     * @param args the array of arguments
     * @return true if arguments are valid, false otherwise
     */
    public static boolean validateArgsBroker(String[] args) {
        //usage: brokerConfig
        if(args.length == 0){
            System.out.println("enter broker config file");
            return false;
        }
        return true;
    }


    /**
     * Validates arguments passed in
     * @param args the array of arguments
     * @return true if arguments are valid, false otherwise
     */
    public static boolean validateArgsLoadBalancer(String[] args) {
        //3 5 (3 broker and 5 partitions) brokerConfig
        if(args.length == 0){
            System.out.println("enter number of broker and partition");
            return false;
        }
        else if (args.length < 3){
            System.out.println("missing another argument");
            return false;
        }
        else if (args.length > 3){
            System.out.println("invalid number of arguments");
            return false;
        }
        return true;
    }


    /**
     * randomly generate numbers in range
     * @param start, end
     * @return the randomly selected number
     */
    public static int randomGen(int start, int end){
        Random randomGenerator = new Random();
        return randomGenerator.nextInt(end) + start;
    }


    /**
     * read config for host name and port
     * @return an object contains maps
     */
    public static List<Object> readConfig(){
        IPMap ipMap = new IPMap();
        PortMap portMap = new PortMap();
        HostInfo hostInfo;
        List<Object> output = new ArrayList<>();
        // read config.json to hostMap
        Gson gson = new Gson();
        String line;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(Utilities.HostConfigFileName), StandardCharsets.ISO_8859_1))) {
            while ((line = br.readLine()) != null) {
                if ((!line.equals(""))) {
                    try { //skip bad line
                        hostInfo = gson.fromJson(line, HostInfo.class);
                        ipMap.put(hostInfo.getHost_id(), hostInfo.getIp_address());
                        portMap.put(hostInfo.getHost_id(), hostInfo.getPort_number(), hostInfo.getPort_number_rep());

                    } catch (JsonSyntaxException e) {
                        System.out.println("skip a bad line...");
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("fail to read the file");
            e.printStackTrace();
        }
        output.add(ipMap);
        output.add(portMap);
        return output;
    }



    /**
     * read config for host name and port
     * @return an object contains maps
     */
    public static List<Object> readBrokerConfig(String BrokerConfigFileName){
        IPMap ipMap = new IPMap();
        PortMap portMap = new PortMap();
        HostInfo hostInfo;
        List<Object> output = new ArrayList<>();
        // read config.json to hostMap
        Gson gson = new Gson();
        String line;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(BrokerConfigFileName), StandardCharsets.ISO_8859_1))) {
            while ((line = br.readLine()) != null) {
                if ((!line.equals(""))) {
                    try { //skip bad line
                        hostInfo = gson.fromJson(line, HostInfo.class);
                        ipMap.put(hostInfo.getHost_id(), hostInfo.getIp_address());
                        portMap.put(hostInfo.getHost_id(), hostInfo.getPort_number(), hostInfo.getPort_number_rep());

                    } catch (JsonSyntaxException e) {
                        System.out.println("skip a bad line...");
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("fail to read the file");
            e.printStackTrace();
        }
        output.add(ipMap);
        output.add(portMap);
        return output;
    }

    /**
     * read config for host name and port
     * @return an object contains maps
     */
    public static List<Integer> readInfoMap(String topic, int startingPosition){
        List<Integer> output = new ArrayList<>();
        String line;
        int brokerID = -1;
        int partitionID = -1;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(Utilities.InfoFileName), StandardCharsets.ISO_8859_1))) {
            while ((line = br.readLine()) != null) {
                if ((!line.equals(""))) {
                    try { //skip bad line
                        String[] splitLine = line.split(",");
                        if (splitLine[0].equalsIgnoreCase(String.valueOf(startingPosition))){
                           if(splitLine[2].equalsIgnoreCase(topic)){
                               brokerID = Integer.parseInt(splitLine[4]);
                               partitionID = Integer.parseInt(splitLine[3]);
                               output.add(brokerID);
                               output.add(partitionID);
                               return output;
                           }
                        }
                    } catch (JsonSyntaxException e) {
                        System.out.println("skip a bad line...");
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("fail to read the file");
            e.printStackTrace();
        }
        return output;
    }


    /**
     * get offsets by msg id
     */
    public static int getBytesOffsetById(int id, String offsetFilePath){
        // id,offset
        int offset;
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(offsetFilePath));
            String line = reader.readLine();
            while (line != null){
                if(line.startsWith(String.valueOf(id))){
                    offset = Integer.parseInt(line.split(",")[1]);
                    return offset;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    /**
     * get msg id by offset
     */
    public static int getIdByOffset(int offset, String offsetFilePath){
        // id,offset
        int id;
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(offsetFilePath));
            String line = reader.readLine();
            while (line != null){
                if(line.endsWith(String.valueOf(offset))){
                    id = Integer.parseInt(line.split(",")[0]);
                    return id;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    /**
     * get hashed key
     */
    public static int hashKey(String key){
        return key.hashCode();
    }

    /**
     * calculate the partition id
     */
    public static int CalculatePartition(String key, int numOfPartitions){
        int hashCode = hashKey(key);
        return Math.abs(hashCode % numOfPartitions + 1); // partition starts with 1
    }


    /**
     * calculate broker id
     */
    public static int CalculateBroker(int partition, int numOfBrokers){
      //  return partition % numOfBrokers; // broker starts with 1
            Random randomGenerator = new Random();
            return randomGenerator.nextInt(numOfBrokers) + 1;

    }

    /**
     * get broker id from broker config file
     */
    public static int getBrokerIDFromFile(String brokerHostName, String brokerPort, String brokerConfigFile){
        String line;
        int brokerID = -1;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(brokerConfigFile), StandardCharsets.ISO_8859_1))) {
            while ((line = br.readLine()) != null) {
                if ((!line.equals(""))) {
                    try { //skip bad line
                        String[] splitLine = line.replace("\"", "").replace("}", "").replace(" ", "").split(",");
                        if (splitLine[1].split(":")[1].equalsIgnoreCase(brokerHostName)){
                            if(splitLine[2].split(":")[1].equalsIgnoreCase(brokerPort)
                                    || splitLine[3].split(":")[1].equalsIgnoreCase(brokerPort) ){
                                brokerID = Integer.parseInt(splitLine[0].split(":")[1]);
                                return brokerID;
                            }
                        }
                    } catch (JsonSyntaxException e) {
                        System.out.println("skip a bad line...");
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("fail to read the file");
            e.printStackTrace();
        }

        return brokerID;
    }



    /**
     * write bytes to files
     */
    public static void writeBytesToFile(String fileOutput, byte[] buf)
            throws IOException {
        try (FileOutputStream fos = new FileOutputStream(fileOutput, true)) {
            System.out.println("Application is storing data to the file...");
            fos.write(buf);
            fos.write(10);
            fos.flush();
        }
        catch(IOException e){
            System.out.println("file writing error :(");
        }
    }




    public static void sendMembershipTableUpdates(Connection connLB, String type, int senderID, int peerID,
                                                  String peerHostName, int peerPort, String token, boolean isLeader, boolean isAlive){
        BrokerToLoadBalancer.lb table = BrokerToLoadBalancer.lb.newBuilder()
                .setType(type)
                .setSenderID(senderID)
                .setBrokerID(peerID)
                .setHostName(peerHostName)
                .setPort(peerPort)
                .setToken(token)
                .setIsLeader(isLeader)
                .setIsAlive(isAlive)
                .build();
        connLB.send(table.toByteArray());
        switch (type) {
            case "new" -> System.out.println("Lead broker sent NEW MEMBER updates to Load Balancer ... \n");
            case "updateLeader" -> System.out.println("Lead broker sent LEADER updates to Load Balancer ... \n");
            case "updateAlive" -> System.out.println("Lead broker sent ALIVE updates to Load Balancer ... \n");
        }
    }



    public static void leaderConnectToLB(String LBHostName, int LBPort, String senderHostName, int senderPort, Connection connLB){
        System.out.println("Connected to LB : " + LBHostName + ":" + LBPort);
        String type = "broker";
        PeerInfo.Peer peerInfo = PeerInfo.Peer.newBuilder()
                .setType(type)
                .setHostName(senderHostName)
                .setPortNumber(senderPort)
                .build();
        connLB.send(peerInfo.toByteArray());
        System.out.println("Lead broker sent peer info to Load Balancer ... \n");
    }



    public static String getHostnameByID(int id){
        List<Object> maps = Utilities.readBrokerConfig(brokerConfigFile);
        IPMap ipMap = (IPMap) maps.get(0);
        String peerHostName = ipMap.getIpById(String.valueOf(id));
        return peerHostName;
    }

    public static int getPortByID(int id){
        List<Object> maps = Utilities.readBrokerConfig(brokerConfigFile);
        PortMap portMap = (PortMap) maps.get(1);
        int peerPort = Integer.parseInt(portMap.getPortById(String.valueOf(id)));
        return peerPort;
    }

    public static int getPortRepByID(int id){
        List<Object> maps = Utilities.readBrokerConfig(brokerConfigFile);
        PortMap portMap = (PortMap) maps.get(1);
        int peerPort = Integer.parseInt(portMap.getPortRepById(String.valueOf(id)));
        return peerPort;
    }

    public static String convertMapToString(ConcurrentHashMap<Integer, MemberInfo> membershipTable) {
        ConcurrentHashMap<Integer, MemberInfo> map = membershipTable;
        String mapAsString = map.keySet().stream()
                .map(key -> key + "=" + map.get(key))
                .collect(Collectors.joining(", ", "{", "}"));
        return mapAsString;
    }


//    public static Map<Integer, MemberInfo> convertStringToMap(String mapAsString) {
//        Map<Integer, MemberInfo> map = Arrays.stream(mapAsString.split(","))
//                .map(entry -> entry.split("="))
//                .collect(Collectors.toMap(entry -> entry[0], entry -> entry[1]));
//        return map;
//
//    }
    public static MembershipTable convertStringToMap(ByteString byteStr) {
//        MembershipTable m = new MembershipTable();
//        byte[] byteStr = str.toByteArray();
//        m = (MembershipTable)SerializationUtils.deserialize(
//                byteArray);
//
//        Object obj = new Object();
//        ObjectInputStream bin;
//        try {
//            bin = new ObjectInputStream(new ByteArrayInputStream(byteStr));
//            obj = bin.readObject();
//        } catch (IOException | ClassNotFoundException exception) {
//            exception.printStackTrace();
//        }
//        return (MembershipTable) obj;

        String[] tokens = byteStr.toString().split(",");
        MembershipTable map = new MembershipTable();
        for (int i = 0; i < tokens.length - 1; i++) {
            tokens[i] = tokens[i].replace("{","")
                    .replace("}","")
                    .replace(" ","");
            String[] elements = tokens[i].split("=");
            int id = Integer.parseInt(elements[0]); // key
            String s = elements[1]; //val
            System.out.println("memberInfo: " + s);
            String[] items = s.split(",");
            //System.out.println(items);
            MemberInfo m = new MemberInfo(null, 0, null, false, false);
            for(int j = 0; j < items.length - 1; j++) {
                if (j == 0) { //hostname
                    String[] splitted = items[j].split("=");
                    String hostName = splitted[0];
                    String hostNameVal = splitted[1];
                    m.setHostName(hostNameVal);
                }
                if (j == 1) { //port
                    String[] splitted = items[j].split("=");
                    String port = splitted[0];
                    int portVal = Integer.parseInt(splitted[1]);
                    m.setPort(portVal);
                }
                if (j == 2) { //tokens
                    String[] splitted = items[j].split("=");
                    String token = splitted[0];
                    String tokenVal = splitted[1];
                    m.setToken(tokenVal);
                }
                if (j == 3) { //isLeader
                    String[] splitted = items[j].split("=");
                    String isLeader = splitted[0];
                    boolean isLeaderVal = Boolean.parseBoolean(splitted[1]);
                    m.setLeader(isLeaderVal);
                }
                if (j == 4) { //isALive
                    String[] splitted = items[j].split("=");
                    String isAlive = splitted[0];
                    boolean isAliveVal = Boolean.parseBoolean(splitted[1]);
                    m.setAlive(isAliveVal);
                }
            }
            map.put(id, m);
        }
        return map;


    }

}
