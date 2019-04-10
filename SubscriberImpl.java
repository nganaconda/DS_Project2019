import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

public class SubscriberImpl implements Subscriber
{
    public static Socket requestSocket = null;
    //private ObjectOutputStream out;
    //private ObjectInputStream in;
    public int port;
    public String ip;
    public boolean registered = false;
    //private Info info;

    public static void main(String[] args) {
        SubscriberImpl sub = new SubscriberImpl("192.168.56.1", 1234);
        sub.connect();
        sub.ask("821");

    }

    public SubscriberImpl(String ipnew, int portnew) {
        ip = ipnew;
        port = portnew;
        requestSocket = null;
    }


    @Override
    public void init(int i) {



    }


    @Override
    public void connect() {
        for (Broker b : brokers) {
            ObjectOutputStream out = null;
            ObjectInputStream in = null;
            String message;
            try {
                requestSocket = new Socket(InetAddress.getByName(b.getIp()), b.getPort());
                out = new ObjectOutputStream(requestSocket.getOutputStream());
                in = new ObjectInputStream(requestSocket.getInputStream());

                try {
                    //to message einai ena string oti h sundesh egine epituxws
                    //to noBrokers einai ena int gia to posoi einai oi brokers
                    //to id einai to id tou ka8e broker pros enhmerwsh
                    //to topic einai ena string me ola ta topic tou ka8e broker
                    message = (String) in.readObject();
                    int noBrokers = (int) in.readObject();
                    int id;
                    String topic;
                    for (int i = 0; i < noBrokers; i++) {
                        id = (Integer) in.readObject();
                        topic = (String) in.readObject();
                        ArrayList<Topic> top = new ArrayList<Topic>();
                        for (int j = 0; j < topic.split(" ").length; j++) {
                            top.add(new Topic(topic.split(" ")[j]));
                        }
                        Broker bro = brokers.get(id);
                        bro.setTopics(top);
                        brokers.set(id, bro);
                    }
                    for (Broker br : brokers) {
                        System.out.println(br.getPort() + ": ");
                        for (Topic t : br.getTopics()) {
                            System.out.print(t.getBusLineId());
                            System.out.print(" ");
                        }
                        System.out.println();
                    }

                    System.out.println("Broker > " + message);

                    /*out.writeObject("821");
                    out.flush();

                    out.writeObject("bye");
                    out.flush();

                    String finalreply = (String) in.readObject();
                    System.out.println(finalreply);*/
                } catch (ClassNotFoundException classNot) {
                    System.out.println("data received in unknown format");
                }
            } catch (UnknownHostException unknownHost) {
                System.err.println("You are trying to connect to an unknown host!");
            } catch (IOException ioException) {
                ioException.printStackTrace();
            } /*finally {
                try {
                    in.close();
                    out.close();
                    requestSocket.close();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }*/
        }
    }

    public void disconnect(Socket requestSocket) {
//        try {
//            in.close();
//            out.close();
//            requestSocket.close();
//        } catch (IOException ioException) {
//            ioException.printStackTrace();
//        }
    }


    public void ask(String line) {
        ObjectInputStream in = null;
        ObjectOutputStream out = null;
        boolean matched = false;

        for (Broker b : brokers) {
            try {
                requestSocket = new Socket(InetAddress.getByName(b.getIp()), b.getPort());
                out = new ObjectOutputStream(requestSocket.getOutputStream());
                in = new ObjectInputStream(requestSocket.getInputStream());
                for (Topic t : b.getTopics()) {
                    if (t.getBusLineId().equals(line)) {
                        matched = true;
                        out.writeObject(line);
                        out.flush();

                        out.writeObject("bye");
                        out.flush();

                        String finalreply = (String) in.readObject();
                        System.out.println(finalreply);
                    }
                }
                if (matched != true) {
                    out.writeObject("bye");
                    out.flush();

                }
                out.writeObject("bye");
                out.flush();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

}
