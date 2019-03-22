package DS_as1;
import java.io.*;
import java.net.*;


public class BrokerImpl implements Broker
{
    private String ip;
    private int port;

    public BrokerImpl(String ipnew, int portnew)
    {
        ip = ipnew;
        port = portnew;
    }

    public static void main(String[] args)
    {
        BrokerImpl bi = new BrokerImpl("192.168.56.1", 4321);
        brokers.add(bi);
        bi.connectToPub();
        bi.connectToSub();
    }

    /*
        Υπεύθυνη για την επικοινωνία με τον/τους Publishers. Επικοινωνεί με το port που ακούει
        ο Pub, του γράφει πως η σύνδεση έχει καλώς και του γράφει bye για να κλείσει η σύνδεση.
     */
    public void connectToPub()
    {
        Socket  requestSocket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        String publisher;
        String subscriber;
        try
        {
            requestSocket = new Socket(InetAddress.getByName(ip), port);
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            try
            {
                publisher = (String) in.readObject();
                System.out.println("Server > " + publisher);

                out.writeObject(ip + Integer.toString(port));
                out.flush();

                out.writeObject("bye");
                out.flush();
            }
            catch(ClassNotFoundException classNot)
            {
                System.out.println("data received in unknown format");
            }
            /*
             */
        }
        catch (UnknownHostException unknownHost)
        {
            System.err.println("You are trying to connect to an unknown host!");
        }
        catch (IOException ioException)
        {
            ioException.printStackTrace();
        }
        finally
        {
            try
            {
                in.close();
                out.close();
                requestSocket.close();
            }
            catch (IOException ioException)
            {
                ioException.printStackTrace();
            }
        }
    }

    /*
        Υπεύθυνη για την επικοινωνία με τον/τους Subscribers. Περιμένει σε ένα port (διαφορετικό
        από αυτό στο οποίο ακούει ο Publisher, και μόλις συνδεθεί ένας client τον ενημερώνει ότι
        η σύνδεση έχει καλώς και περιμένει ένα string "bye" για να κλείσει η σύνδεση.
     */
    public void connectToSub()
    {
        ServerSocket providerSocket = null;
        Socket connection = null;
        String message = null;
        try {
            providerSocket = new ServerSocket(4324);

            while (true) {
                connection = providerSocket.accept();
                ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(connection.getInputStream());

                out.writeObject("Broker successfully connected to Client. ");
                out.flush();
                do {
                    try {
                        message = (String) in.readObject();
                        System.out.println(connection.getInetAddress().getHostAddress() + "> " + message);

                    } catch (ClassNotFoundException classnot) {
                        System.err.println("Data received in unknown format");
                    }
                } while (/***/!message.equals("bye"));
                in.close();
                out.close();
                connection.close();
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            try {
                providerSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

}
