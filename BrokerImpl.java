package DS_as1;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.*;
import javax.xml.bind.DatatypeConverter;


public class BrokerImpl extends Thread implements Broker {

    public String brokerID;
    private Socket requestSocket = null;
    private ObjectOutputStream out = null;
    private ObjectInputStream in = null;
    private ObjectOutputStream outS = null;
    private ObjectInputStream inS = null;
    public String ip = "192.168.1.6";
    public String port;
    private ServerSocket replySocket = null;
    public ArrayList<Topic> topics = new ArrayList<Topic>();
    private static List<BrokerImpl> broker;
    public int hashipport;
    public static final PublisherImpl publisher = new PublisherImpl("192.168.1.7", 4321);
    public static final SubscriberImpl subscriber = new SubscriberImpl();
   //public List<PublisherImpl> registeredPublishers = new ArrayList<PublisherImpl>();


    public BrokerImpl(String IDnew) {
        brokerID = IDnew;
    }

    public void run() {
        System.out.println("Broker with ID " + brokerID + " started.");

        System.out.println(this.brokerID + ": ");
        for(Topic t : this.topics)
        {
            System.out.print(t.getBusLine() + " ");
        }

        this.acceptConnection(publisher);
        String topic = "";
        for(Topic t : this.topics)
        {
            topic += t.getBusLine();
            topic += " ";
        }
        this.notifyPublisher(topic);
        this.notifyPublisher("bye");

        this.acceptConnection(subscriber);
        Topic topicAsked = this.getInfo();

        for(Topic t : this.topics){
            if(topicAsked.getBusLine().equals(t.getBusLine()))
            {
                System.out.println(this.brokerID + " EGW");
                this.notifyPublisher(t.getBusLine());
            }
        }
    }

    public int getPort() {
        return Integer.parseInt(this.port);
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


    public static void main(String[] args) {
        int noBrokers = Integer.parseInt(args[0]);
        if(noBrokers < 1) System.out.println("You selected to start no Broker. ");
        else
        {
            broker = new ArrayList<BrokerImpl>();
            for(int i = 0; i < noBrokers; i++)
            {
                broker.add(new BrokerImpl(Integer.toString(i)));
            }

            File buslines = new File("DS_project_dataset/busLinesNew.txt");
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

                    for (BrokerImpl b : broker) {
                        b.init(3);
                        if (hashtopic <= b.hashipport && b.hashipport < nearestNode) {
                            nearestNode = b.hashipport;
                        }
                    }
                    for (BrokerImpl b : broker) {
                        if (b.hashipport == nearestNode) b.topics.add(new Topic(lineCode));
                    }
                }
            }
            catch (IOException e)
            {
                System.out.println("Error reading busLinesNew.txt .");
            }

            for(BrokerImpl bi : broker)
            {
                /*System.out.println("Broker " + bi.brokerID + ":");
                for(Topic t : bi.topics)
                {
                    System.out.print(" " + t.getBusLine() + " ");
                }
                System.out.println("\n");*/
                bi.start();

                /*try
                {
                    bi.join();
                }
                catch (Exception e)
                {
                    System.out.println("Interrupted Thread. ");
                }*/
            }
        }
    }

    @Override
    public void init(int x) {
        calculateKeys();
    }

    public void connect() {
    }

    public void calculateKeys() {
        /*String port = Integer.toString(requestSocket.getLocalPort());
        String ip = requestSocket.getLocalAddress().toString();*/
        port = Integer.toString(Integer.parseInt(brokerID) + 1000);
        hashipport = (ip + port).hashCode();
    }

    public void acceptConnection(PublisherImpl pub) {
        String publisher;
        try {
            requestSocket = new Socket(InetAddress.getByName("192.168.1.6"), 4321);
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            try {
                publisher = (String) in.readObject();
                System.out.println("\nServer > " + publisher);

                out.writeObject("Broker with id " + brokerID + " connected.");
                out.flush();

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


    public void acceptConnection(SubscriberImpl sub) {
        Socket connection = null;
        String message = null;
        try {
            replySocket = new ServerSocket(Integer.parseInt(port));
            while(true) {
                connection = replySocket.accept();
                outS = new ObjectOutputStream(connection.getOutputStream());
                inS = new ObjectInputStream(connection.getInputStream());

                outS.writeObject("Broker " + brokerID + " successfully connected to Client.");
                outS.flush();
                break;
                /*do {
                    try {
                        message = (String) in.readObject();
                        System.out.println(connection.getInetAddress().getHostAddress() + "> " + message);

                    } catch (ClassNotFoundException classnot) {
                        System.err.println("Data received in unknown format");
                    }
                } while (!message.equals("bye"));
                in.close();
                out.close();
                connection.close();*/
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } /*finally {
            try {
                replySocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }*/
    }


    public void notifyPublisher(Object msg) {
        try {
            out.writeObject(msg);
            out.flush();
        } catch (UnknownHostException unknownHost) {
            System.err.println("You are trying to connect to an unknown host!");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
        /*if(msg.equals("bye")){
            try {
                in.close();
                out.close();
                requestSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }*/
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
        /*try {
            inS.close();
            out.close();
            replySocket.close();
        }
        catch (IOException ioException) {
            ioException.printStackTrace();
        }*/
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
}