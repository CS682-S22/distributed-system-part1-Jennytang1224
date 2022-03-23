import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * A utility class for reading config files and getting ip and hostname
 */
public class Utilities {

    private static String HostConfigFileName = "files/config.json";
    static String InfoFileName = "files/InfoMap";
    static String offsetFilePath = "files/idMapOffset";
    private static String hostname;
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
    public static boolean validateArgs(String[] args) {
        if(args.length != 7){
            System.out.println("Invalid number of arguments");
            System.out.println("Usage: Request test.txt from host1 loss to /folder1/");
            return false;
        }
        if(!args[0].equalsIgnoreCase("request")){
            System.out.println("Invalid action");
            return false;
        }
        if(!args[2].equalsIgnoreCase("from")){
            System.out.println("Invalid syntax");
            return false;
        }

        if(!(args[4].equalsIgnoreCase("default") || args[4].equalsIgnoreCase("loss"))){
            System.out.println("Invalid connection type");
            return false;
        }
        if(!args[5].equalsIgnoreCase("to")){
            System.out.println("Invalid syntax");
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
                        portMap.put(hostInfo.getHost_id(), hostInfo.getPort_number());

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
                        portMap.put(hostInfo.getHost_id(), hostInfo.getPort_number());

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
                            if(splitLine[2].split(":")[1].equalsIgnoreCase(brokerPort)){
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
}
