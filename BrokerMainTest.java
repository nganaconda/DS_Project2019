import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class BrokerMainTest implements Broker {


    @Override
    public void calculateKeys() {

    }

    @Override
    public Publisher acceptConnection(Publisher p) {
        return null;
    }

    @Override
    public Subscriber acceptConnection(Subscriber p) {
        return null;
    }

    @Override
    public void notifyPublisher(String s) {

    }

    @Override
    public void pull(Topic t) {

    }

    @Override
    public void init(int i) {

    }

    @Override
    public void connect() {

    }

    @Override
    public void disconect(Socket requestSocket, ObjectInputStream in, ObjectOutputStream out) {

        try {
            in.close();
            out.close();
            requestSocket.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    @Override
    public void updateNodes() {

    }

    public static void main(String[] args) {
        new BrokerMainTest().startBroker();



    }


    public void startBroker() {
        Socket requestSocket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        String message;

        try {
            requestSocket = new Socket(InetAddress.getByName("192.168.56.1"), 4321);

            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());



                try {
                    message = (String) in.readObject();
                    System.out.println("Broker: " + message);

                    out.writeObject("Client successfully connected to Broker. ");
                    out.flush();

                    out.writeObject("bye");
                    out.flush();

                } catch (ClassNotFoundException classNot) {
                    System.out.println("Data received in unknown format");
                }


        } catch (UnknownHostException unknownHost) {
            System.err.println("You are trying to connect to an unknown host!");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }




}
