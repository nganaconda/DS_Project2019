package DS_as1;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

public class SubscriberImpl implements Subscriber {
    public int port;
    public String ip;
    public Socket requestSocket;
    public boolean registered = false;
    private Info info;

    public SubscriberImpl(String ipnew, int portnew) {
        ip = ipnew;
        port = portnew;
        requestSocket = null;
    }

    public static void main(String[] args) {
        SubscriberImpl sub = new SubscriberImpl("192.168.1.6", 1234);
        sub.connect();
        sub.ask("040");
        sub.ask("021");
    }


    @Override
    public void init(int x) {

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
                    Info<Topic> topic = new Info<>();
                    for (int i = 0; i < noBrokers; i++) {
                        id = (Integer) in.readObject();
                        topic = (Info<Topic>) in.readObject();
                        ArrayList<Topic> top = new ArrayList<Topic>();
                        top = topic;
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

    public void ask(String line) {
        ObjectInputStream in = null;
        ObjectOutputStream out = null;

        for (Broker b : brokers) {
            try {
                for (Topic t : b.getTopics()) {
                    if (t.getBusLineId().equals(line)) {
                        requestSocket = new Socket(InetAddress.getByName(b.getIp()), b.getPort());
                        out = new ObjectOutputStream(requestSocket.getOutputStream());
                        in = new ObjectInputStream(requestSocket.getInputStream());

                        out.writeObject(line);
                        out.flush();

                        Tuple<Value> finalreply = (Tuple<Value>) in.readObject();
                        for(Value v : finalreply){
                            System.out.println("\n" + v.getBus().getInfo() + " " + line + " " + v.getBus().getRouteCode() + " " + v.getBus().getLineCode() + " " + v.getBus().getVehicleId() + " " + v.getLatitude() + " " + v.getLongitude());
                        }
                    }
                }
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
