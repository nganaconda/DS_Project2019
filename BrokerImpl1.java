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

public class BrokerImpl1 extends Thread implements Broker, Serializable
{
    private int id;
    private int port;
    private String ip;
    private int hashipport;     //A hash of a string containing the ip and the port of the Broker
    private int noPublishers = 0;
    private ServerSocket requestSocket;
    private ServerSocket replySocket = null;
    public Socket connection = null;
    private ObjectOutputStream out = null;      //out is used to communicate with a Publisher
    private ObjectOutputStream outS = null;     //outS is used to communicate with a Subscriber
    private ObjectInputStream in = null;        //in is used to communicate with a Publisher
    private ObjectInputStream inS = null;       //inS is used to communicate with a Subscriber
    private static Info info;
    public ArrayList<Topic> topics = new ArrayList<Topic>();
    public ArrayList<Value> values = new ArrayList<Value>();
    private List<PublisherImpl> registeredPublishers = new ArrayList<PublisherImpl>()
    {
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

    public void setRequestSocket(ServerSocket soc) {
        this.requestSocket = soc;
    }

    public ServerSocket getRequestSocket(){
        return this.requestSocket;
    }



    public void run() {
        /* First the Thread establishes a connection with each Publisher in the registeredPublishers list and informs them of the
         * topics that it is responsible for. */
        try {
            requestSocket = new ServerSocket(port+1);
        } catch (IOException e) {
            e.printStackTrace();
        }


        Thread t1 = new Thread(){
            public void run() {
                while(true) {
                    try {
                        Socket conn = requestSocket.accept();
                        out = new ObjectOutputStream(conn.getOutputStream());
                        in = new ObjectInputStream(conn.getInputStream());

                        if(in.readObject().equals(" ")){
                            pull(new Topic(null));
                        }
                        else {
                            PublisherImpl p = new PublisherImpl(registeredPublishers.size());
                            p.socket = conn;
                            acceptConnection(p);
                            Info<Topic> topic = new Info<>();
                            for (Topic top : topics) {
                                topic.add(top);
                            }
                            notifyPublisher(topic);
                        }

                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        t1.start();

        /* The Thread now awaits for a new request from a Subscriber. That request is a String that will be used to create a new Topic
         * named topicAsked. */

        try {
            replySocket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }

        while(true) {
            SubscriberImpl s = null;
            this.acceptConnection(s);

            while (true) {
                try {
                    Socket connection = replySocket.accept();
                    outS = new ObjectOutputStream(connection.getOutputStream());
                    inS = new ObjectInputStream(connection.getInputStream());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                Topic topicAsked = this.getInfo();
                if(topicAsked.getBusLineId().equals("bye")){
                    break;
                }

                Tuple<Value> tuple = new Tuple<>();

                for (Topic t : this.topics) {
                    if (topicAsked.getBusLineId().equals(t.getBusLineId())) {
                        while(true) {
                            for (Value value : values) {
                                if (value.getBus().getLineCode().equals(t.getLineCode())) {
                                    tuple.add(value);
                                }
                            }
                            if(tuple.size() > 0){
                                break;
                            }
                        }
                        try {
                            outS.writeObject(tuple);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }

            }
        }
    }

    /* This is the main method. A Broker Thread from the list of Brokers provided in the Node class is initialized and starts. */
    public static void main(String[] args)
    {
        int id = Integer.parseInt(args[0]);
        BrokerImpl b =(BrokerImpl) brokers.get(id);
        b.init(id);
        b.start();
    }

    /* This method uses the MD5 algorithm to calculate a hash for the ip+port String of a Thread. */
    @Override
    public void calculateKeys() {
        String portS = Integer.toString(this.port);
        String hashippor = ip + portS;
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

    /* This method is used to establish a connection with a given Publisher and to inform the said Publisher of the id, ip and port
     * of the current Thread. */
    @Override
    public void acceptConnection(PublisherImpl pub) {
        String publisher;
        registeredPublishers.add(pub);
        try {
            out.writeObject(pub.getID());
            out.writeObject(this.registeredPublishers.size());
            System.out.println("Publisher " + pub.getID() + " connected. NumOfPubs is " + registeredPublishers.size());

            publisher = (String) in.readObject();
            System.out.println("\nServer > " + publisher);


            out.writeObject(id + " " + ip + " " + port);
            out.flush();
        } catch (ClassNotFoundException classNot) {
            System.out.println("data received in unknown format");
        }
        catch (IOException ioException) {
        }

    }

    /* This method is used to establish a connection with a given Subscriber and to inform the said Subscriber of all the topics that
     * each Broker is responsible for. */
    @Override
    public void acceptConnection(SubscriberImpl sub)
    {
        Socket connection = null;
        try {
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

                outS.writeObject(brokers.size());
                for(Broker b : brokers) {
                    Info<Topic> topics = new Info<>();
                    outS.writeObject(b.getID());
                    for(Topic t : b.getTopics()){
                        topics.add(t);
                    }
                    outS.writeObject(topics);
                }
                break;
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    /* This method is used to inform a Publisher that is already connected with the current Broker Thread of a given Object. */
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

    /* This method is used to obtain a String of a buslineID that a Subscriber already connected to the current Broker Thread
     * and create a Topic object based on that buslineID. */
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

    /* This method is used to obtain a Tuple of Values based on a Topic from a Publisher that is already connected to the current
     * Broker Thread if the said Publisher is responsible for that Topic. If he is, then he first sends a String "Yes", and then
     * the Tuple. */
    @Override
    public void pull(Topic topic) {
        try {
            out.writeObject(registeredPublishers.size());
            Value value = (Value) in.readObject();
            boolean found = false;

            for(Topic t : topics){
                if(t.getLineCode().equals(value.getBus().getLineCode())){
                    for(Value val : values){
                        if(val.getBus().getLineCode().equals(value.getBus().getLineCode())) {
                            if (val.getBus().getRouteCode().equals(value.getBus().getRouteCode())) {
                                found = true;
                                values.set(values.indexOf(val), value);
                            }
                        }
                    }

                    if(found == false){
                        values.add(value);
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


        /*try{
            Tuple<Value> repl = (Tuple<Value>) in.readObject();
            for(int i = 0; i < values.size(); i++){
                if(values.get(i).get(0).getBus().getLineCode().equals(repl.get(0).getBus().getLineCode())){
                    values.set(i, repl);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }*/


       /* Tuple<Value> reply;
        try {
            String repl = (String) in.readObject();
            System.out.println(repl);
            if(repl.equals("Yes")) {
                reply = (Tuple<Value>) in.readObject();
                outS.writeObject(reply);
                outS.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }*/
    }

    /* This method is used in order for the current Broker Thread to Hash the Topics or buslineIDs found on busLines.txt
     * between the Brokers. Method MD5 is used for the hashing and some more constraints are used in order to insure that
     * all Brokers get an equal or almost equal number of Topics. */
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
            String desc;

            while ((line = br.readLine()) != null) {
                lineCode = line.split(",")[0];
                busID = line.split(",")[1];
                desc = line.split(",")[2];

                /* This is where a hash is created based on a buslineID using the MD5 digestion. */
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

                /* Each topic must be assigned to a Broker when the hash of its ip+port is greater or equal to the hash of
                 * the buslineID. The nearest node is the one that the difference between its hash of ip+port is the closest
                 * to that of the hash of the buslineID. */
                int nearestNode = Integer.MAX_VALUE;


                for (Broker b : brokers) {
                    if (hashtopic <= b.getHashipport() && b.getHashipport() < nearestNode) {
                        nearestNode = b.getHashipport();
                    }
                }

                /* If the hash of the buslineID is greater than all of the hashes of the Brokers, then we set the nearestNode
                 * as the broker who has the less topics assigned already. */
                if(nearestNode == Integer.MAX_VALUE){
                    int min = Integer.MAX_VALUE;
                    for(Broker b : brokers){
                        if(b.getTopics().size() < min){
                            min = b.getTopics().size();
                            nearestNode = b.getHashipport();
                        }
                    }
                }

                /* We ensure that no Broker has more than one Topic than all the others. */
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

                /* If a buslineID is already assigned to a Broker, then the rest of the lineCodes sharing the same
                 * buslineID are also assigned to the same Broker. */
                for(Broker b : brokers){
                    for(Topic t : b.getTopics()){
                        if(busID.equals(t.getBusLineId())){
                            nearestNode = b.getHashipport();
                        }
                    }
                }

                /* We finally add the Topic to the appropriate Broker */
                for (Broker b : brokers) {
                    if (b.getHashipport() == nearestNode) b.addTopics(new Topic(lineCode, busID, desc));
                }
            }
            for(Broker b : brokers){
                System.out.println("Broker " + b.getPort() + ":");
                for(Topic t : b.getTopics())
                {
                    System.out.print(t.getBusLineId() + " ");
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

    /* This method is used to close the connection with all connected Nodes. */
    public void disconnect() {
        for (PublisherImpl p : registeredPublishers) {
            try {
                Socket conn = requestSocket.accept();
                in = new ObjectInputStream(conn.getInputStream());
                out = new ObjectOutputStream(conn.getOutputStream());

                in.close();
                out.close();
                requestSocket.close();

                inS.close();
                outS.close();
                replySocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }
}
