import java.io.*;
import java.net.Socket;

/**
 * a wrapper class for the socket, and has receive and send methods
 */
public class Connection {
    private int port;
    private String hostName;
    private Socket socket;
    private DataInputStream input;
    private DataOutputStream output;
    private int receiverCounter = 0;

    public Connection(String hostName, int port){
        this.hostName = hostName;
        this.port = port;
        socket = null;
        try {
            socket = new Socket(this.hostName, this.port);
            input = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Connection(Socket socket){
        this.socket = socket;
        try  {
            this.input = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            this.output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * implement receiver interface
     * @return byte array received
     */

    public byte[] receive()  {
        byte[] buffer = null;

        try {
            int length = input.readInt();
            if(length > 0) {
                buffer = new byte[length];
                input.readFully(buffer, 0, buffer.length);
            }
        } catch (EOFException ignored) {} //No more content available to read
        catch (IOException exception) {

            System.err.printf(" Fail to receive message ");
        }

        return buffer;
    }

        /**
     * implement sender interface
     * @return if send successfully
     */

    public boolean send(byte[] message) {
            writeToSocket(message);
            return true;
    }

    /**
     * write data to socket

     */
    public void writeToSocket(byte[] message){
        try {
            this.output.writeInt(message.length);
            this.output.write(message);
            this.output.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getHostName(){
        return hostName;
    }

    public int getPort(){
        return port;
    }


}
