import java.io.*;
import java.net.Socket;

public class Producer {
    private String brokerLocation;
    private int brokerPort;
    private String brokerHostName;
    private Socket socket;
    private DataInputStream input;
    private DataOutputStream output;


    public Producer(String brokerLocation) {
        this.brokerLocation = brokerLocation;
        this.brokerHostName =brokerLocation.split(":")[0];
        this.brokerPort = Integer.parseInt(brokerLocation.split(":")[1]);
        this.socket = null;
        try {
            this.socket = new Socket(this.brokerHostName, this.brokerPort);
            this.input = new DataInputStream(new BufferedInputStream(this.socket.getInputStream()));
            this.output = new DataOutputStream(new BufferedOutputStream(this.socket.getOutputStream()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public Producer(Socket socket){
        this.socket = socket;
        try  {
            this.input = new DataInputStream(new BufferedInputStream(this.socket.getInputStream()));
            this.output = new DataOutputStream(new BufferedOutputStream(this.socket.getOutputStream()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * implement sender interface
     * @return if send successfully
     */

    public void send(byte[] record) {

        writeToSocket(record);

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

    public void close(){

    }



}
