import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * A utility class for reading config files and getting ip and hostname
 */
public class Utilities {

    public static String HostConfigFileName = "src/config.json";
    private static String hostname;

    /**
     * get computer host name
     */
    public static String getHostName(){
        try {
            hostname = InetAddress.getLocalHost().getHostName();
            //System.out.println("Hostname : " + hostname);
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


}
