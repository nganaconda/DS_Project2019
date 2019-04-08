package DS_as1;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

public class SubscriberImpl implements Subscriber
{
    private static Socket requestSocket = null;
    private Info info;

    public static void main(String[] args)
    {
        SubscriberImpl sub = new SubscriberImpl();
        sub.connect();
        sub.ask("821");
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
                    for(int i = 0; i < noBrokers; i++) {
                        id = (Integer) in.readObject();
                        topic = (String) in.readObject();
                        ArrayList<Topic> top = new ArrayList<Topic>();
                        for(int j = 0; j < topic.split(" ").length; j++){
                            top.add(new Topic(topic.split(" ")[j]));
                        }
                        Broker bro = brokers.get(id);
                        bro.setTopics(top);
                        brokers.set(id, bro);
                    }
                    for(Broker br : brokers){
                        System.out.println(br.getPort() + ": ");
                        for(Topic t : br.getTopics()){
                            System.out.print(t.getBusLine());
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

    public void ask(String line){
        ObjectInputStream in = null;
        ObjectOutputStream out = null;
        boolean matched = false;

        for(Broker b : brokers){
            for(Topic t : b.getTopics()){
                if(t.getBusLine().equals(line)){
                    matched = true;
                    try {
                        requestSocket = new Socket(InetAddress.getByName(b.getIp()), b.getPort());
                        out = new ObjectOutputStream(requestSocket.getOutputStream());
                        in = new ObjectInputStream(requestSocket.getInputStream());

                        try {
                            out.writeObject(line);
                            out.flush();

                            out.writeObject("bye");
                            out.flush();

                            String finalreply = (String) in.readObject();
                            System.out.println(finalreply);
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            if(matched != true) {
                try {
                    requestSocket = new Socket(InetAddress.getByName(b.getIp()), b.getPort());
                    out = new ObjectOutputStream(requestSocket.getOutputStream());
                    in = new ObjectInputStream(requestSocket.getInputStream());
                    try {
                        out.writeObject("bye");
                        out.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            try {
                requestSocket = new Socket(InetAddress.getByName(b.getIp()), b.getPort());
                out = new ObjectOutputStream(requestSocket.getOutputStream());
                try {
                    out.writeObject("bye");
                    out.flush();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
