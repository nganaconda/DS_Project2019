package DS_as1;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.*;
import javax.xml.bind.DatatypeConverter;


public class BrokerImpl extends Thread implements Broker {
    public String brokerID;
    private Socket requestSocket = null;
    public String ip = "192.168.1.12";
    public String port;
    private ServerSocket replySocket = null;
    public List<Topic> topics = new ArrayList<Topic>();
    private static List<BrokerImpl>  broker;
    public int hashipport;
    public static PublisherImpl publisher = new PublisherImpl("1");
   //public List<PublisherImpl> registeredPublishers = new ArrayList<PublisherImpl>();


    public BrokerImpl(String IDnew) {
        brokerID = IDnew;
    }

    public void run() {
        System.out.println("Thread " + brokerID + " started.");
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
                System.out.println("ID " + i);
            }

            for(BrokerImpl bi : broker)
            {
                bi.start();
                bi.init(3);
                try
                {
                    bi.join();
                }
                catch (Exception e)
                {
                    System.out.println("Interrupted Thread. ");
                }
            }


            File buslines = new File("DS_project_dataset/busLinesNew.txt");
            try
            {
                FileReader fr = new FileReader(buslines);
                BufferedReader br = new BufferedReader(fr);

                String line = br.readLine();
                String busID;
                String lineCode;

                while((line = br.readLine()) != null)
                {
                    busID = line.split(",")[1];
                    lineCode = line.split(",")[0];

                    int hashtopic = 0;
                    String sha1 = null;
                    try {
                        MessageDigest msdDigest = MessageDigest.getInstance("SHA-1");
                        msdDigest.update(busID.getBytes("UTF-8"), 0, busID.length());
                        sha1 = DatatypeConverter.printHexBinary(msdDigest.digest());
                        hashtopic = Integer.parseInt(sha1);
                    } catch (UnsupportedEncodingException | NoSuchAlgorithmException e) {
                    }

                    System.out.println("hash tou " + busID + ": " + hashtopic);
                    int nearestNode = Integer.MAX_VALUE;

                    for(BrokerImpl b : broker)
                    {
                        System.out.println("hash tou ip port tou " + b.brokerID + ": " + b.hashipport);
                        if(hashtopic <= b.hashipport && b.hashipport < nearestNode)
                        {
                            nearestNode = b.hashipport;
                        }
                    }
                    for(BrokerImpl b : broker) {
                        if(b.hashipport == nearestNode) b.topics.add(new Topic(lineCode));
                    }
                }
                for(BrokerImpl b : broker)
                {
                    System.out.println(b.brokerID + ": ");
                    for(Topic t : b.topics)
                    {
                        System.out.print(t.getBusLine() + ", ");
                    }
                    System.out.print("\n");
                }
                /*for(BrokerImpl b : broker)
                {
                    b.acceptConnection(publisher);
                    String topic = "";
                    for(Topic t : b.topics)
                    {
                        topic += t.getBusLine();
                        topic += ",";
                    }
                    b.notifyPublisher(topic);
                }*/
            }
            catch (IOException e)
            {
                System.out.println("Error reading busLinesNew.txt .");
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
        port = Integer.toString(Integer.parseInt(brokerID));
        hashipport = (ip + "/" + port).hashCode();
    }

    public void acceptConnection(Publisher pub) {
        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        String publisher;
        try {
            requestSocket = new Socket(InetAddress.getByName("192.168.1.12"), 4321);
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            try {
                publisher = (String) in.readObject();
                System.out.println("Server > " + publisher);

                out.writeObject("Thread with id " + brokerID + " and hash " + hashipport);
                out.flush();

                out.writeObject("bye");
                out.flush();
            } catch (ClassNotFoundException classNot) {
                System.out.println("data received in unknown format");
            }
        } catch (UnknownHostException unknownHost) {
            System.err.println("You are trying to connect to an unknown host!");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            try {
                in.close();
                out.close();
                requestSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }


    public void acceptConnection(Subscriber sub) {
        Socket connection = null;
        String message = null;
        try {
            replySocket = new ServerSocket(4324);
            while (true) {
                connection = replySocket.accept();
                ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(connection.getInputStream());

                out.writeObject("Broker successfully connected to Client. ");
                out.flush();
                do {
                    try {
                        message = (String) in.readObject();
                        System.out.println(connection.getInetAddress().getHostAddress() + "> " + message);

                    } catch (ClassNotFoundException classnot) {
                        System.err.println("Data received in unknown format");
                    }
                } while (!message.equals("bye"));
                in.close();
                out.close();
                connection.close();
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            try {
                replySocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }


    public void notifyPublisher(String msg) {
        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        try {
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());
            try {
                out.writeObject(msg);
                out.flush();
            } catch (UnknownHostException unknownHost) {
                System.err.println("You are trying to connect to an unknown host!");
            } catch (IOException ioException) {
                ioException.printStackTrace();
            } finally {
                try {
                    in.close();
                    out.close();
                    requestSocket.close();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}