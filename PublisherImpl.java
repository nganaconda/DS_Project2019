package DS_as1;

import com.sun.security.ntlm.Server;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PublisherImpl extends Thread implements Publisher
{
    public int port;
    public String ip;
    private int id;
    private ServerSocket providerSocket = null;
    public Socket socket;
    ObjectOutputStream out;
    ObjectInputStream in;
    private ArrayList<Bus> buses = new ArrayList<>();
    private ArrayList<Value> values = new ArrayList<>();
    private ArrayList<Topic> topics = new ArrayList<>();
    private static ArrayList<PublisherImpl> publishers = new ArrayList<PublisherImpl>();

    public PublisherImpl(String ipnew, int portnew, int idnew)
    {
        ip = ipnew;
        port = portnew;
        id = idnew;
    }

    public void run(){
        this.connect();
    }


    public static void main(String[] args)
    {
        //new PublisherImpl("192.168.1.6", 4321).connect();
        int numOfPubs = Integer.parseInt(args[0]);
        if(numOfPubs == 0){
            System.out.println("You have chosen to run no Publishers. ");
        }
        else{
            for(int i = 0; i < numOfPubs; i++) {
                publishers.add(new PublisherImpl("192.168.1.6", 4321+i, i));
            }
            for(PublisherImpl p : publishers){
                p.start();
            }
        }
    }

    @Override
    public void init(int x) {

    }

    public void connect()
    {
        Socket connection = null;
        String message = null;
        try {
            providerSocket = new ServerSocket(port);

                for(int j = 0; j < brokers.size(); j++) {
                    try {
                        connection = providerSocket.accept();
                        out = new ObjectOutputStream(connection.getOutputStream());
                        in = new ObjectInputStream(connection.getInputStream());

                        out.writeObject("Server " + this.id + " successfully connected to Broker. ");
                        out.flush();

                        //to message 8a prepei na periexei to id, to ip kai to port tou broker me ton opoio exoume anoi3ei sundesh, ola xwrismena me keno
                        message = (String) in.readObject();
                        /*if(message.equals("bye")){
                            break;
                        }*/
                        System.out.println(connection.getInetAddress().getHostAddress() + "> " + message);
                        int id = Integer.parseInt(message.split(" ")[0]);
                        String ipB = message.split(" ")[1];
                        String portB = message.split(" ")[2];
                        Broker b = new BrokerImpl1(id, ipB, Integer.parseInt(portB));
                        b.setRequestSocket(connection);

                        //ousiastika enhmerwnoume th lista me tous brokers me ta topics gia ta opoia einai upeu8unos o ka8enas
                        /*String topic = (String) in.readObject();
                        ArrayList<Topic> top = new ArrayList<Topic>();
                        for(int i = 0; i < topic.split(" ").length; i++){
                            top.add(new Topic(topic.split(" ")[i]));
                        }*/
                        Info<Topic> topic = (Info<Topic>) in.readObject();
                        ArrayList<Topic> top = topic;
                        b.setTopics(top);
                        b.setRequestSocket(connection);
                        brokers.set(id, b);
                        System.out.println("Broker " + b.getPort() + ":");
                        for(Topic t : b.getTopics())
                        {
                            System.out.print(t.getBusLine() + " ");
                        }
                        System.out.println("\n");

                    } catch (ClassNotFoundException classnot) {
                        System.err.println("Data received in unknown format");
                    }
                }

            while(true) {
                //edw einai h proswrinh ekdosh ths pull giati gia twra douleuoume me strings
                    try {
                        connection = providerSocket.accept();
                        out = new ObjectOutputStream(connection.getOutputStream());
                        in = new ObjectInputStream(connection.getInputStream());
                        Topic newtopic = (Topic) in.readObject();
                        String reply = "821,1804,10015,37.985427,23.75514,Mar  4 2019 10:39:00:000AM";
                        out.writeObject(reply);

                        /*in.close();
                        out.close();
                        connection.close();*/
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } /*finally {
                try {
                    providerSocket.close();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }*/

    }
}
