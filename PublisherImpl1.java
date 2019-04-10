

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class PublisherImpl1 extends Thread implements Publisher {

    private static ArrayList<Topic> topicsA = new ArrayList<>(); // topicsA is for the first half of the file busLinesNew.txt

    private ObjectOutputStream out;
    private ObjectInputStream in;
    private static ArrayList<Bus> buses = new ArrayList<>();
    private static ArrayList<Value> values = new ArrayList<>();
    private HashMap<Topic,Value> tv = new HashMap<>();          // for push method
    private HashSet<Value> vs = new HashSet<>();             // for push method
    MyList<Value> m = new MyList<Value>();

    private int publisherId = 0;
    private String ip;
    private int port;
    private ServerSocket s = null;
    public Socket socket;


    public PublisherImpl1(int publisherId) {
        this.publisherId = publisherId;

    }

    public PublisherImpl1(String ipnew, int portnew) {
        ip = ipnew;
        port = portnew;
    }

    public PublisherImpl1() {

    }


    public void getBrokerList() {

    }


    public void hashTopic(Topic t) { // Hashes only one Topic



    }

    public void run() { // MUST SEE
        // publisher.start()
        init(0);
        connect();

    }

    public void push(String lineId) {
        try{
            Topic t;
            Value v;

            for(int i =0;i<topicsA.size();i++) {  // Publisher checks if he has the BusLineId he was asked
                if(lineId.equals(topicsA.get(i).getLineCode())) {
                    t = topicsA.get(i);
                    out.writeObject(t); // I send the LineId/Topic to the broker
                    for(int k =0;k<values.size();k++) {
                        if(values.get(k).getBus().getLineCode().equals(lineId)) {
                            v = values.get(k);
                            //out.writeObject(v); // I send the Value for the specified lineCode
                            tv.put(t,v);
                            vs.add(v);
                        }

                    }
                    out.writeObject(tv); // I send the hashmap with the topic and the values

                }
            }
        }catch (IOException e) {
            e.printStackTrace();
            notifyFailure();
        }

    }


    public void notifyFailure() {

        try {
            out.writeObject("Publisher node not responding!!!");
            out.flush();
        }catch (IOException e) {
            e.printStackTrace();
        }

    }


    public void init(int i) {

        int lineNumber = 1;
        StringBuilder text = new StringBuilder();

        try{
            File f1 = new File("C:\\Users\\Owner\\AndroidStudioProjects\\src\\busLinesNew.txt");
            BufferedReader br1 = new BufferedReader(new FileReader(f1));
            String line;
            String[] myLine;
            while(lineNumber<=10) {

                line = br1.readLine();
                text.append(line + "\n");
                myLine = line.split(",");
                System.out.println("myLine " + myLine[0]);
                Topic t = new Topic(myLine[0],myLine[1],myLine[2]);
                topicsA.add(t);

                lineNumber++;
            }


            File f2 = new File("C:\\Users\\Owner\\AndroidStudioProjects\\src\\busPositionsNew.txt");
            BufferedReader br2 = new BufferedReader(new FileReader(f2));
            StringBuilder text2 = new StringBuilder();

            while((line=br2.readLine()) != null) {
                text2.append(line + "\n");
                myLine = line.split(",");
                Bus b = new Bus(myLine[0],myLine[1],myLine[2],myLine[5]);
                buses.add(b);
                Value v = new Value(b,Double.parseDouble(myLine[3]),Double.parseDouble(myLine[4]));
                values.add(v);
            }


        }catch (IOException e) {
            e.printStackTrace();
        }

    }



    public void connect() { // Sends data and messages to brokers.
        ServerSocket providerSocket = null;
        Socket connection = null;
        String message = null;
        try {
            providerSocket = new ServerSocket(4321); // Create new server socket
            System.out.println("Publisher's id is: "  + this.getPublisherId());

            while (true) {

                for(Broker b : brokers) {

                    connection = providerSocket.accept(); // wait for connection
                    out = new ObjectOutputStream(connection.getOutputStream());
                    in = new ObjectInputStream(connection.getInputStream());

                    System.out.println("Broker id is: " + b.getBrokerId());

                    out.writeObject("Publisher successfully connected to Broker.");
                    out.flush();

                    message = (String) in.readObject(); // The id-ip-port
                    if(message.equals("bye")){
                        break;
                    }
                    //System.out.println(connection.getInetAddress().getHostAddress() + "> " + message);

                    int id = Integer.parseInt(message.split(" ")[0]);
                    String ipB = message.split(" ")[1];
                    String portB = message.split(" ")[2];

                    Broker br = new BrokerImpl1(id, ipB, Integer.parseInt(portB)); // substitutes an already existed broker

                    String topic = (String) in.readObject(); // I get the topic
                                                             // topic: the topic that the broker is responsible for

                    ArrayList<Topic> top = new ArrayList<Topic>(); // top list is for the brokers
                    for(int i = 0; i < topic.split(" ").length; i++) { // topic: is a string of many topics of 1 broker
                        top.add(new Topic(topic.split(" ")[i]));
                    }
                    br.setTopics(top);
                    brokers.set(id, br);
                    System.out.println("Broker " + br.getPort() + ":");

                    for(Topic t : br.getTopics()) {
                        System.out.print(t.getBusLineId() + " ");
                    }
                    System.out.println("\n");

                    //  in.close();
                    //  out.close();
                    //  connection.close();
                }

                while(true) {

                    String topic = (String) in.readObject(); // I get the topic from the broker. That topic was asked from the subscriber though.

                    System.out.println("The asked Topic from the subscriber is: " + topic);

                    push(topic); // topic is the busLineId

                    disconnect(connection);
                    break;
                }


                //message = (String) in.readObject();
                //System.out.println(connection.getInetAddress().getHostAddress() + ">" + message);

                //System.out.println(in.readUTF());
                //System.out.println((Message) in.readObject());
                /*
                do {
                    try {
                        message = (String) in.readObject();
                        System.out.println(connection.getInetAddress().getHostAddress() + ">" + message);
                        //Thread.sleep(10000); // 10 sec
                    } catch (ClassNotFoundException classnot) {
                        System.err.println("Data received in unknown format");
                    }
                } while (!message.equals("bye")); */

                //disconnect(connection,in,out);

            } // while(true)

            //disconnect(connection,in,out);
        } catch (IOException r) {
            r.printStackTrace();
        }catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        finally {
            try {
                providerSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }



    }


    public void disconnect(Socket requestSocket) {
        try {
            in.close();
            out.close();
            requestSocket.close();

        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }




    public static void main(String[] args) {
       new PublisherImpl1("192.168.56.1",4321).start();


        /*
        int pubsNum = Integer.parseInt(args[0]);
        if(pubsNum==0) {
            System.out.println("Error! Cannot have 0 publishers.");
        }else {
            ArrayList<PublisherImpl1> pubs = new ArrayList<>();
            for(int i=0;i<pubsNum;i++) {
                pubs.add(new PublisherImpl1(i+1));

            }
            for(PublisherImpl1 p: pubs) {
                p.start();
                try{
                    //p.join();
                    p.init(p.getPublisherId());
                    p.connect();
                    System.out.println("Hi");


                }catch (Exception e) {
                    System.out.println("Thread interrupted");
                    e.printStackTrace();
                }
            }

         }

        int lineNumber = 1;
        StringBuilder text1 = new StringBuilder();

        try {
            File f1 = new File("C:\\Users\\Owner\\AndroidStudioProjects\\src\\busLinesNew.txt");
            File f2 = new File("C:\\Users\\Owner\\AndroidStudioProjects\\src\\busPositionsNew.txt");
            BufferedReader br1 = new BufferedReader(new FileReader(f1));
            String line;
            String[] myLine;


            while((line = br1.readLine())!= null) {


                text1.append(line + "\n");
                myLine = line.split(",");
                //System.out.println("myLine " + myLine[0]);
                Topic t = new Topic(myLine[0],myLine[1],myLine[2]);
                topics.add(t);

                lineNumber++;
            }

            System.out.println("LineNumber " + lineNumber);
            System.out.println("Topics arraylist size is: " + topics.size());
            System.out.println("End of reading BusLinesNew.txt....");



            BufferedReader br2 = new BufferedReader(new FileReader(f2));
            StringBuilder text2 = new StringBuilder();



            lineNumber = 1;
            while(lineNumber <= 12500) {

                line = br2.readLine();
                text2.append(line + "\n");
                myLine = line.split(",");
                //System.out.println("myLine " + myLine[0]);
                //System.out.println("myLine[0]: " + myLine[0] + " myLine[1]: " + myLine[1] +" myLine[2]: " + myLine[2] + " myLine[3] " + Double.parseDouble(myLine[3]) + " myLine[4] " + Double.parseDouble(myLine[4]) + " myLine[5]: " + myLine[5]);
                Bus b = new Bus(myLine[0],myLine[1],myLine[2],myLine[5]);
                buses.add(b);
                Value v = new Value(b,Double.parseDouble(myLine[3]),Double.parseDouble(myLine[4]));
                values.add(v);

                lineNumber++;
            }

            System.out.println("LineNumber " + lineNumber);
            System.out.println("Buses arraylist size is: " + buses.size());
            System.out.println("Values arraylist size is: " + values.size());
            System.out.println("\nEnd of reading BusPositionsNew.txt....");


            System.out.println("End of PublisherMain1....");
            br1.close();
            br2.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
        //new PublisherMain1().openServer();
        */

    }


    public int getPublisherId() {
        return publisherId;
    }

    public void setPublisherId(int publisherId) {
        this.publisherId = publisherId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }


//    public Socket getSocket() {
//        return socket;
//    }
}
