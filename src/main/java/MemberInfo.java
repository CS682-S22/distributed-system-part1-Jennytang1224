public class MemberInfo {
    String hostName;
    int port;
    String token;
    boolean isLeader;
    boolean isAlive;

    public MemberInfo(String hostName, int port, String token, boolean isLeader, boolean isAlive) {
        this.hostName = hostName;
        this.port = port;
        this.token = token;
        this.isLeader = isLeader;
        this.isAlive = isAlive;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public boolean isLeader() {
        return isLeader;
    }

    public void setLeader(boolean leader) {
        isLeader = leader;
    }

    public boolean isAlive() {
        return isAlive;
    }

    public void setAlive(boolean alive) {
        isAlive = alive;
    }


}
