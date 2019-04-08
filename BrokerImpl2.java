

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BrokerImpl2 extends Thread implements Broker
{
    public int id = 1;
    private int port;
    private String ip;
    private int hashipport;
    private Socket requestSocket = null;
    private ServerSocket replySocket = null;
    private ObjectOutputStream out = null;
    private ObjectOutputStream outS = null;
    private ObjectInputStream in = null;
    private ObjectInputStream inS = null;
    public ArrayList<Topic> topics = new ArrayList<Topic>();
    private List<PublisherImpl1> registeredPublishers = new ArrayList<PublisherImpl1>()
    {
        {add(new PublisherImpl1("192.168.56.1", 4321));}
    };
    private List<SubscriberImpl> registeredSubscribers = new ArrayList<SubscriberImpl>()
    {
        {add(new SubscriberImpl());}
    };


    public BrokerImpl2(String ipnew, int portnew)
    {
        ip = ipnew;
        port = portnew;
        id = 0;
    }

    public int getPort() {
        return this.port;
    }

    public String getIp() {
        return this.ip;
    }

    public int getHashipport(){
        return this.hashipport;
    }

    public void addTopics(Topic t){
        this.topics.add(t);
    }

    public ArrayList<Topic> getTopics(){
        return this.topics;
    }

    public void setTopics(ArrayList<Topic> t){
        this.topics = t;
    }

    public int getBrokerId() {
        return this.id;
    }

    public void run()
    {
        for(PublisherImpl1 p : registeredPublishers)
        {
            this.acceptConnection(p);
            String topic = "";
            for(Topic t : this.topics){
                topic += t.getBusLineId();
                topic += " ";
            }
            this.notifyPublisher(topic);
            this.notifyPublisher("bye");
            for(SubscriberImpl s : registeredSubscribers) {
                this.acceptConnection(s);
                Topic topicAsked = this.getInfo();

                for (Topic t : this.topics) {
                    if (topicAsked.getBusLineId().equals(t.getBusLineId())) {
                        System.out.println(this.port + " EGW");
                        this.notifyPublisher(t.getBusLineId());
                    }
                }
            }
        }
    }

    public static void main(String[] args)
    {
        File buslines = new File("C:\\Users\\Owner\\AndroidStudioProjects\\src\\busLinesNew.txt");
        for (Broker b : brokers) {
            b.calculateKeys();
        }
        try {
            FileReader fr = new FileReader(buslines);
            BufferedReader br = new BufferedReader(fr);

            String line = br.readLine();
            String busID;
            String lineCode;

            while ((line = br.readLine()) != null) {
                busID = line.split(",")[1];
                lineCode = line.split(",")[0];

                int hashtopic = busID.hashCode();
                int nearestNode = Integer.MAX_VALUE;

                while(true) {
                    for (Broker b : brokers) {
                        if (hashtopic <= b.getHashipport() && b.getHashipport() < nearestNode) {
                            nearestNode = b.getHashipport();
                        }
                    }
                    if(nearestNode == Integer.MAX_VALUE){
                        hashtopic = brokers.get(0).getHashipport();
                    }
                    else{
                        break;
                    }
                }
                for (Broker b : brokers) {
                    if (b.getHashipport() == nearestNode) b.addTopics(new Topic(lineCode));
                }
            }
            for(Broker b : brokers){
                System.out.println("Broker " + b.getPort() + ":");
                for(Topic t : b.getTopics())
                {
                    System.out.print(" " + t.getBusLineId() + " ");
                }
                System.out.println("\n");
            }
        }
        catch (IOException e)
        {
            System.out.println("Error reading busLinesNew.txt .");
        }

        BrokerImpl1 b =(BrokerImpl1) brokers.get(1);
        b.start();
    }

    @Override
    public void calculateKeys() {
        String portS = Integer.toString(this.port);
        this.hashipport = (ip + portS).hashCode()%10000 * 5;
    }

    @Override
    public void acceptConnection(Publisher pub) {
        String publisher;
        try {
            requestSocket = new Socket(pub.getIp(), pub.getPort());
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            //this.port = requestSocket.getLocalPort();

            try {
                publisher = (String) in.readObject();
                System.out.println("\nServer > " + publisher);

                out.writeObject(id + " " + ip + " " + port);
                out.flush();

            } catch (ClassNotFoundException classNot) {
                System.out.println("data received in unknown format");
            }
        } catch (UnknownHostException unknownHost) {
            System.err.println("You are trying to connect to an unknown host!");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    @Override
    public void acceptConnection(SubscriberImpl sub)
    {
        Socket connection = null;
        String message = null;
        try {
            replySocket = new ServerSocket(port);
            while(true) {
                connection = replySocket.accept();
                outS = new ObjectOutputStream(connection.getOutputStream());
                inS = new ObjectInputStream(connection.getInputStream());

                outS.writeObject("Broker " + port + " successfully connected to Client.");
                outS.flush();
                break;
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    @Override
    public void notifyPublisher(Object msg) {
        try {
            out.writeObject(msg);
            out.flush();
        } catch (UnknownHostException unknownHost) {
            System.err.println("You are trying to connect to an unknown host!");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    @Override
    public Topic getInfo() {
        String msg;
        Topic topic = null;
        while(true) {
            try {
                msg = (String) inS.readObject();
                System.out.println(msg);
                if (msg.equals("bye")) {
                    break;
                }
                topic = new Topic(msg);

            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        return topic;
    }

    @Override
    public void pull(Topic topic) {
        HashMap<Topic, Value> topicValueHashMap;
        while(true) {
            try {
                topicValueHashMap = (HashMap<Topic, Value>) in.readObject();
                outS.writeObject(topicValueHashMap);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void init() {

    }

    @Override
    public void connect() {

    }

    public void disconnect(Socket requestSocket) {
        for (PublisherImpl1 p : registeredPublishers) {
            try {
                out.writeObject("bye");
                in.close();
                out.close();
                requestSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }
}
