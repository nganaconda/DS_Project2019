import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;


public class PublisherImpl2 extends Thread implements Publisher {

    private static ArrayList<Topic> topics = new ArrayList<>();
    private static ArrayList<Bus> buses = new ArrayList<>();
    private static ArrayList<Value> values = new ArrayList<>();
    private static ArrayList<Topic> topicsB = new ArrayList<>(); // topicsB is for the second half of the file busLinesNew.txt
    private HashMap<Topic,Value> tv = new HashMap<>();          // for push method

    private ObjectOutputStream out;
    private ObjectInputStream in;
    private int publisherId = 1;
    private String ip;
    private int port;
    private ServerSocket s = null;
    private Socket socket;


    public PublisherImpl2(int publisherId) {
        this.publisherId = publisherId;

    }

    public PublisherImpl2(String ipnew, int portnew) {
        ip = ipnew;
        port = portnew;
    }

    /*
    @Override
    public void getBrokerList() {

    }

    @Override
    public void hashTopic(Topic t) {
        //return null;
    }
    */

    public void run() { // MUST SEE
        // publisher.start()
        init(0);
        connect();

    }

    @Override
    public void push(String lineId) {
        try{

            Topic t;
            Value v;

            for(int i =0;i<topicsB.size();i++) {
                if(lineId.equals(topicsB.get(i).getLineCode())) {
                    t = topicsB.get(i);
                    out.writeObject(t); // I send the LineId/Topic to the broker
                    for(int k =0;k<values.size();k++) {
                        if(values.get(k).getBus().getLineCode().equals(lineId)) {
                            v = values.get(k);
                            //out.writeObject(v); // I send the Value for the specified lineCode
                            tv.put(t,v);
                        }
                    }
                    out.writeObject(tv); // I send the hashmap with the topic and the values
                }

            }


        }catch (IOException e) {
            e.printStackTrace();
        }



    }

    @Override
    public void notifyFailure() {

        try {
            out.writeObject("Publisher node not responding!!!");
            out.flush();
        }catch (IOException e) {
            e.printStackTrace();
        }

    }



    @Override
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

                lineNumber++;
            }
            System.out.println("\n");

            while((line=br1.readLine()) != null) {

                text.append(line + "\n");
                myLine = line.split(",");
                if(lineNumber==11) {
                    System.out.println("myLine " + myLine[0]);
                    Topic t = new Topic(myLine[0],myLine[1],myLine[2]);
                    topicsB.add(t);
                }
                while(lineNumber>=11 && (line=br1.readLine()) != null) {
                    text.append(line + "\n");
                    myLine = line.split(",");
                    System.out.println("myLine " + myLine[0]);
                    Topic t = new Topic(myLine[0],myLine[1],myLine[2]);
                    topicsB.add(t);

                    lineNumber++;
                }

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

    @Override
    public void connect() {

        ServerSocket providerSocket = null;
        Socket connection = null;
        String message = null;
        try {
            providerSocket = new ServerSocket(4000); // Create new server socket
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
                //in.close();
                //out.close();
                //connection.close();
            } // while(true)

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                providerSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }




    }

    @Override
    public void disconnect(Socket requestSocket) {
        try {
            in.close();
            out.close();
            requestSocket.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }



    public int getPublisherId() {
        return publisherId;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public static void main(String[] args) {

        new PublisherImpl2("192.168.56.1",4000).start();

       /*
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
                System.out.println("myLine " + myLine[0]);
                Topic t = new Topic(myLine[0],myLine[1],myLine[2]);
                topics.add(t);

                lineNumber++;
            }
            System.out.println("\nLinenumber " + lineNumber);
            System.out.println("Topics list size " + topics.size() + "\n");
            System.out.println("End of reading BusLinesNew.txt....");





            BufferedReader br2 = new BufferedReader(new FileReader(f2));
            StringBuilder text2 = new StringBuilder();

            lineNumber = 1;
            while((line=br2.readLine()) != null) {
                text2.append(line + "\n");
                myLine = line.split(",");
                if(lineNumber == 11965) {
                    Bus b = new Bus(myLine[0],myLine[1],myLine[2],myLine[5]);
                    //System.out.println("myLine[0]: " + myLine[0] + " myLine[1]: " + myLine[1] +" myLine[2]: " + myLine[2] + " myLine[5]: " + myLine[5]);
                    buses.add(b);
                    Value v = new Value(b,Double.parseDouble(myLine[3]),Double.parseDouble(myLine[4]));
                    //System.out.println("Latitude: " + Double.parseDouble(myLine[3]) + " longtitude: " + Double.parseDouble(myLine[4]));
                    values.add(v);
                }
                while(lineNumber >= 11966 && (line=br2.readLine()) != null) {
                    text2.append(line + "\n");
                    myLine = line.split(",");
                    Bus b = new Bus(myLine[0],myLine[1],myLine[2],myLine[5]);
                    buses.add(b);
                    Value v = new Value(b,Double.parseDouble(myLine[3]),Double.parseDouble(myLine[4]));
                    values.add(v);


                    lineNumber++;
                }


                lineNumber++;
            }

            System.out.println("\nLinenumber " + lineNumber);
            System.out.println("Buses list size " + buses.size());
            System.out.println("Values list size " + values.size());
            System.out.println("\nEnd of reading BusPositionsNew.txt....");




            System.out.println("End of PublisherImpl2....");
            br1.close();
            br2.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
        */

    }


    public Socket getSocket() {
        return socket;
    }
}
