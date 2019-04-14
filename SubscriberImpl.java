package DS_as1;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Scanner;

public class SubscriberImpl implements Subscriber {
    public int port;
    public String ip;
    public Socket requestSocket;
    public boolean registered = false;
    private Info info;
    private ObjectOutputStream out;
    private ObjectInputStream in;

    public SubscriberImpl(String ipnew, int portnew) {
        ip = ipnew;
        port = portnew;
        requestSocket = null;
    }

    public static void main(String[] args) {
        SubscriberImpl sub = new SubscriberImpl("192.168.2.13", 1234);
        sub.connect();
        Scanner reader = new Scanner(System.in);
        int x = 0;
        while (x != 2) {
            System.out.println("--------------------------");
            System.out.println("| Menu                   |");
            System.out.println("| 1. Search for a bus.   |");
            System.out.println("| 2. Quit.               |");
            System.out.println("--------------------------");
            System.out.println("Choose your option: ");
            x = reader.nextInt();
            if (x == 1) {
                System.out.println("Enter the bus's line id: ");
                String line = reader.next();
                sub.visualiseData(sub.ask(line), line);
            }
            if (x == 2) {
                break;
            }
        }
        sub.disconnect();
    }


    @Override
    public void init(int x) {

    }

    @Override
    public void connect() {
        for (Broker b : brokers) {
            out = null;
            in = null;
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

    public Tuple<Value> ask(String line) {
        ObjectInputStream in = null;
        ObjectOutputStream out = null;
        Tuple<Value> finalreply = new Tuple<Value>();

        for (Broker b : brokers) {
            try {
                for (Topic t : b.getTopics()) {
                    if (t.getBusLineId().equals(line)) {
                        requestSocket = new Socket(InetAddress.getByName(b.getIp()), b.getPort());
                        out = new ObjectOutputStream(requestSocket.getOutputStream());
                        in = new ObjectInputStream(requestSocket.getInputStream());

                        out.writeObject(line);
                        out.flush();

                        finalreply = (Tuple<Value>) in.readObject();
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
        return finalreply;
    }

    public void visualiseData(Tuple<Value> finalreply, String line) {

        for (Value v : finalreply) {
            System.out.println("\n The bus with line id: " + line + " , has route code: " + v.getBus().getRouteCode() + " , line code: " + v.getBus().getLineCode() + " , vehicle's id: " + v.getBus().getVehicleId() + " , latitude: " + v.getLatitude() + " , and longitude: " + v.getLongitude() + " . Latest updated at: " + v.getBus().getInfo());
        }
    }

    public void disconnect(){
        for (Broker b : brokers) {
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
