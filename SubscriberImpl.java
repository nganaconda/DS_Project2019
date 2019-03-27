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
    public static void main(String[] args)
    {
        new SubscriberImpl().connect();
    }

    @Override
    public void connect()
    {
        Socket requestSocket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        String message;
        try {
            requestSocket = new Socket(InetAddress.getByName("192.168.56.1"), 4324);
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            try{
                message = (String) in.readObject();
                System.out.println("Broker > " + message);

                out.writeObject("Client successfully connected to Broker. ");
                out.flush();

                out.writeObject("bye");
                out.flush();
            }
            catch(ClassNotFoundException classNot){
                System.out.println("data received in unknown format");
            }
            /*
             */
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
