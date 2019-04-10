package DS_as1;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BrokerImpl1 extends Thread implements Broker, Serializable
{
    private int id;
    private int port;
    private String ip;
    private int hashipport;
    private Socket requestSocket;
    private ServerSocket replySocket = null;
    public Socket connection;
    private ObjectOutputStream out = null;
    private ObjectOutputStream outS = null;
    private ObjectInputStream in = null;
    private ObjectInputStream inS = null;
    private static Info info;
    public ArrayList<Topic> topics = new ArrayList<Topic>();
    private List<PublisherImpl> registeredPublishers = new ArrayList<PublisherImpl>()
    {
        {add(new PublisherImpl("192.168.1.8", 4321));}
    };
    private List<SubscriberImpl> registeredSubscribers = new ArrayList<SubscriberImpl>()
    {
    };


    public BrokerImpl1(int idnew, String ipnew, int portnew)
    {
        id = idnew;
        ip = ipnew;
        port = portnew;
        requestSocket = null;
    }

    public int getID(){return this.id;}

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

    public void setRequestSocket(Socket soc) {
        this.requestSocket = soc;
    }

    public Socket getRequestSocket(){
        return this.requestSocket;
    }


    public void run()
    {
        //diatrexei th lista me tous publishers kai anoigei sundesh mazi tous
        for(PublisherImpl p : registeredPublishers) {
            this.acceptConnection(p);
            //dhmiourgei ena string me ola ta topics tou trexonta broker kai ta stelnei ston trexonta publishers
            String topic = "";
            for (Topic t : this.topics) {
                topic += t.getBusLine();
                topic += " ";
            }
                this.notifyPublisher(topic);
                /*//to bye edw den kleinei to socket pou einai sundedemeno me ton publisher, apla einai san na tou leei den 8elw kati allo gia twra
                this.notifyPublisher("bye");*/
        }

        boolean allregistered = false;
        //diatrexei th lista me tous subscribers kai anoigei sundesh mazi tous
        for(int i = 0; i < 1; i++){
            SubscriberImpl s = null;
            this.acceptConnection(s);
        }

        while(true) {
            //to topicAsked einai to topic gia to opoio zhtaei plhrofories o Sub
            try{
                Socket connection = replySocket.accept();
                outS = new ObjectOutputStream(connection.getOutputStream());
                inS = new ObjectInputStream(connection.getInputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }
            Topic topicAsked = this.getInfo();
            if (!(topicAsked == null)) {
                //diatrexei ola ta topics tou o trexwn broker kai an einai upeu8unos gia to topic to opoio zhtaei o sub tote enhmerwnei ton antoistoixo pub
                for (Topic t : this.topics) {
                    if (topicAsked.getBusLine().equals(t.getBusLine())) {
                        System.out.println(this.port + " EGW");
                        for(PublisherImpl pub : registeredPublishers) {
                            try {
                                requestSocket = new Socket(pub.ip, pub.port);
                                in = new ObjectInputStream(requestSocket.getInputStream());
                                out = new ObjectOutputStream(requestSocket.getOutputStream());
                            }
                            catch (IOException e){
                                e.printStackTrace();
                            }
                            this.notifyPublisher(pub, t.getBusLine());
                            //to katw einai h proswrinh ekdosh ths pull giati mexri twra xrhsimopoioume Strings
                            String reply;
                            try {
                                reply = (String) in.readObject();
                                outS.writeObject(reply);
                                outS.flush();
                            } catch (IOException e) {
                                e.printStackTrace();
                            } catch (ClassNotFoundException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
            else break;
        }
        for(PublisherImpl p : registeredPublishers){
            //kleinei th sundesh me ton publisher
            try {
                requestSocket = new Socket(p.ip, p.port);
                in = new ObjectInputStream(requestSocket.getInputStream());
                out = new ObjectOutputStream(requestSocket.getOutputStream());
            }
            catch (IOException e){
                e.printStackTrace();
            }
            this.notifyPublisher(p, "bye");
        }
        for(SubscriberImpl s : registeredSubscribers){
            //edw to keno stelnetai ston sub etsi wste an den htan upeu8unos o trexwn broker gia to topic pou zhthse o sub na mhn perimenei o sub
            //adika apanthsh
            try {
                Socket connection = s.requestSocket;
                outS = new ObjectOutputStream(connection.getOutputStream());
                inS = new ObjectInputStream(connection.getInputStream());
                outS.writeObject(" ");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args)
    {
        BrokerImpl1 b =(BrokerImpl1) brokers.get(0);
        b.init(0);
        b.start();
    }

    @Override
    public void calculateKeys() {
        String portS = Integer.toString(this.port);
        this.hashipport = (ip + portS).hashCode()%10000 * 5;
    }

    @Override
    public void acceptConnection(PublisherImpl pub) {
        String publisher;
        try {
            requestSocket = new Socket(pub.ip, pub.port);
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());
            pub.socket = requestSocket;
            for(PublisherImpl p : registeredPublishers){
                if(p.port == pub.port && p.ip.equals(pub.ip)){
                    registeredPublishers.set(registeredPublishers.indexOf(p), pub);
                }
            }

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
        try {
            replySocket = new ServerSocket(port);
            while(true) {
                connection = replySocket.accept();
                String ipS = connection.getInetAddress().getHostAddress();
                int portS = connection.getPort();
                sub = new SubscriberImpl(ipS, portS);
                sub.requestSocket = connection;
                sub.registered = true;
                registeredSubscribers.add(sub);

                outS = new ObjectOutputStream(connection.getOutputStream());
                inS = new ObjectInputStream(connection.getInputStream());

                outS.writeObject("Broker " + port + " successfully connected to Client.");
                outS.flush();

                //leei ston sub posoi einai oi brokers
                outS.writeObject(brokers.size());
                for(Broker b : brokers) {
                    String message = "";
                    //leei ston sub poion broker na enhmerwsei sth lista tou me tous brokers
                    outS.writeObject(b.getID());
                    for(Topic t : b.getTopics()){
                        message += t.getBusLine();
                        message += " ";
                    }
                    //leei ston sub gia poia topics einai upeu8unos o proanaferomenos broker
                    outS.writeObject(message);
                }
                break;
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public void notifyPublisher(Object msg){
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
    public void notifyPublisher(PublisherImpl pub, Object msg) {
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
            try {
                /*Socket connection = replySocket.accept();
                outS = new ObjectOutputStream(connection.getOutputStream());
                inS = new ObjectInputStream(connection.getInputStream());*/

                msg = (String) inS.readObject();
                System.out.println(msg);
                if (msg.equals("bye")) {
                    return null;
                }
                topic = new Topic(msg);

            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
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
    public void init(int x) {
        File buslines = new File("DS_project_dataset/busLinesNew.txt");
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
                    System.out.print(" " + t.getBusLine() + " ");
                }
                System.out.println("\n");
            }
        }
        catch (IOException e)
        {
            System.out.println("Error reading busLinesNew.txt .");
        }
    }

    @Override
    public void connect() {

    }

    public void disconnect() {
        for (PublisherImpl p : registeredPublishers) {
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
