package DS_as1;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BrokerImpl2 extends Thread implements Broker, Serializable
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
        {add(new PublisherImpl("192.168.1.9", 4321, 1));}
        {add(new PublisherImpl("192.168.1.9", 4322, 2));}
    };
    private List<SubscriberImpl> registeredSubscribers = new ArrayList<SubscriberImpl>()
    {
    };

    public BrokerImpl2(int idnew, String ipnew, int portnew)
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
            /*String topic = ""*/;
            Info<Topic> topic = new Info<>();
            for (Topic t : this.topics) {
                /*topic += t.getBusLine();
                topic += " ";*/
                topic.add(t);
            }
            this.notifyPublisher(topic);
        }

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

            //diatrexei ola ta topics tou o trexwn broker kai an einai upeu8unos gia to topic to opoio zhtaei o sub tote enhmerwnei ton antoistoixo pub
            for (Topic t : this.topics) {
                if (topicAsked.getBusLineId().equals(t.getBusLineId())) {
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
                        this.notifyPublisher(t);
                        //to katw einai h proswrinh ekdosh ths pull giati mexri twra xrhsimopoioume Strings
                        Tuple<Value> reply;
                        try {
                            String repl = (String) in.readObject();
                            if(repl.equals("Yes")) {
                                reply = (Tuple<Value>) in.readObject();
                                outS.writeObject(reply);
                                outS.flush();
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
        /*for(PublisherImpl p : registeredPublishers){
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
        }*/
    }

    public static void main(String[] args)
    {
        BrokerImpl1 b =(BrokerImpl1) brokers.get(1);
        b.init(0);
        b.start();
    }

    @Override
    public void calculateKeys() {
        String portS = Integer.toString(this.port);
        String hashippor = ip + portS;
        System.out.println(this.id + " " + hashipport);
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(hashippor.getBytes());
            byte[] digest = md.digest();
            hashipport = DatatypeConverter.printHexBinary(digest).hashCode();
            if(hashipport < 0){
                hashipport = -hashipport;
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
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
                    Info<Topic> topics = new Info<>();
                    //leei ston sub poion broker na enhmerwsei sth lista tou me tous brokers
                    outS.writeObject(b.getID());
                    for(Topic t : b.getTopics()){
                        topics.add(t);
                    }
                    //leei ston sub gia poia topics einai upeu8unos o proanaferomenos broker
                    outS.writeObject(topics);
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
    public Topic getInfo() {
        String msg;
        Topic topic = null;
        try {
            msg = (String) inS.readObject();
            System.out.println(msg);
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

            String line = "";
            String busID;
            String lineCode;
            int lineNumber = 0;

            while ((line = br.readLine()) != null) {
                lineNumber++;
                busID = line.split(",")[1];
                lineCode = line.split(",")[0];

                int hashtopic = 0;
                MessageDigest md = null;
                try {
                    md = MessageDigest.getInstance("MD5");
                    md.update(busID.getBytes());
                    byte[] digest = md.digest();
                    hashtopic = DatatypeConverter.printHexBinary(digest).hashCode();
                    if(hashtopic < 0){
                        hashtopic = - hashtopic;
                    }
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }

                int nearestNode = Integer.MAX_VALUE;


                for (Broker b : brokers) {
                    if (hashtopic <= b.getHashipport() && b.getHashipport() < nearestNode) {
                        nearestNode = b.getHashipport();
                    }
                }

                if(nearestNode == Integer.MAX_VALUE){
                    /*hashtopic = brokers.get(0).getHashipport();*/
                    int min = Integer.MAX_VALUE;
                    for(Broker b : brokers){
                        if(b.getTopics().size() < min){
                            min = b.getTopics().size();
                            nearestNode = b.getHashipport();
                        }
                    }
                }

                int min = Integer.MAX_VALUE;
                Broker mini = brokers.get(0);
                for(Broker b : brokers){
                    if(nearestNode == b.getHashipport()){
                        for(Broker bro : brokers){
                            if(bro.getTopics().size() < b.getTopics().size()){
                                min = bro.getHashipport();
                                mini = bro;
                            }
                        }
                        if(b.getTopics().size() >= mini.getTopics().size()+1){
                            nearestNode = min;
                        }
                    }
                }

                for (Broker b : brokers) {
                    if (b.getHashipport() == nearestNode) b.addTopics(new Topic(busID));
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
