package DS_as1;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

public class SubscriberImpl implements Subscriber
{
    private static Socket requestSocket = null;

    public static void main(String[] args)
    {
        new SubscriberImpl().connect();

    }

    @Override
    public void init(int x) {

    }

    @Override
    public void connect()
    {
        for(Broker b : brokers) {
            ObjectOutputStream out = null;
            ObjectInputStream in = null;
            String message;
            try {
                requestSocket = new Socket(InetAddress.getByName("192.168.1.7"), b.getPort());
                out = new ObjectOutputStream(requestSocket.getOutputStream());
                in = new ObjectInputStream(requestSocket.getInputStream());

                try {
                    message = (String) in.readObject();
                    System.out.println("Broker > " + message);

                    out.writeObject("821");
                    out.flush();

                    out.writeObject("bye");
                    out.flush();
                } catch (ClassNotFoundException classNot) {
                    System.out.println("data received in unknown format");
                }
            } catch (UnknownHostException unknownHost) {
                System.err.println("You are trying to connect to an unknown host!");
            } catch (IOException ioException) {
                ioException.printStackTrace();
            } finally {
                try {
                    in.close();
                    out.close();
                    requestSocket.close();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }
        }
    }
}
