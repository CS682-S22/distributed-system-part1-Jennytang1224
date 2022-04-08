package Project2;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * server class
 */
public class Server {
    private ServerSocket serverSocket;
    private Socket socket;
    private static int port;

    public Server(int port) throws IOException {
        this.port = port;
        this.serverSocket = new ServerSocket(port);
    }

    /**
     * block until new connection
     */
    public Connection nextConnection() {
        try {
            this.socket = this.serverSocket.accept();
            return new Connection(this.socket);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


}
