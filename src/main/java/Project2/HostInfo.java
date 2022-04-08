package Project2;

/**
 * class to store host info
 */
public class HostInfo {
    private String host_id;
    private String ip_address;
    private String port_number;

    public HostInfo(String host_id, String ip_address, String port_number) {
        this.host_id = host_id;
        this.ip_address = ip_address;
        this.port_number = port_number;
    }

    /**
     * getter for id
     */
    public String getHost_id() {
        return this.host_id;
    }

    /**
     * setter for id
     */
    public void setHost_id(String host_id) {
        this.host_id = host_id;
    }

    /**
     * getter for ip
     */
    public String getIp_address() {
        return this.ip_address;
    }

    /**
     * setter for ip
     */
    public void setIp_address(String ip_address) {
        this.ip_address = ip_address;
    }

    /**
     * getter for port number
     */
    public String getPort_number() {
        return this.port_number;
    }

    /**
     * setter for port number
     */
    public void setPort_number(String port_number) {
        this.port_number = port_number;
    }
}
