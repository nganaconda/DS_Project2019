

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class PublisherMain1 extends Thread implements Publisher {

    private static ArrayList<Topic> topicsA = new ArrayList<>(); // topicsA is for the first half of the file busLinesNew.txt
    private static ArrayList<Topic> topicsB = new ArrayList<>(); // topicsB is for the second half of the file busLinesNew.txt
    private static ArrayList<Bus> buses = new ArrayList<>();
    private static ArrayList<Value> values = new ArrayList<>();
    private HashMap<Topic,Value> tv = new HashMap<>();          // for push method

    private int publisherId;
    private String ip;
    private int port;
    private ServerSocket s = null;


    public PublisherMain1(int publisherId) {
        this.publisherId = publisherId;

    }


    public void getBrokerList() {

    }


    public void hashTopic(Topic t) { // Hashes only one Topic



    }


    public void push(String lineCode, ServerSocket providerSocket, Socket connection, ObjectOutputStream out, int j) {
        //File f1 = new File("C:\\Users\\Owner\\AndroidStudioProjects\\src\\busPositionsNew.txt");
        try{

            Topic t;
            //Bus b;
            Value v;

                //for(int h=0;h<buses.size();h++) {
                   // if(buses.get(h).getLineCode().equals(lineCode)) {
                        //b = buses.get(h);
                        //out.writeObject(v);
                        if(j==1) {
                            for(int i =0;i<topicsA.size();i++) {
                                if(lineCode.equals(topicsA.get(i).getLineCode())) {
                                    t = topicsA.get(i);
                                    out.writeObject(t); // I send the LineId/Topic to the broker
                                    for(int k =0;k<values.size();k++) {
                                        if(values.get(k).getBus().getLineCode().equals(lineCode)) {
                                            v = values.get(k);
                                            out.writeObject(v); // I send the Value for the specified lineCode
                                            tv.put(t,v);
                                        }
                                    }

                                }

                            }
                        }else if(j==2) {
                            for(int i =0;i<topicsB.size();i++) {
                                if(lineCode.equals(topicsB.get(i).getLineCode())) {
                                    t = topicsB.get(i);
                                    out.writeObject(t); // I send the LineId/Topic to the broker
                                    for(int k =0;k<values.size();k++) {
                                        if(values.get(k).getBus().getLineCode().equals(lineCode)) {
                                            v = values.get(k);
                                            out.writeObject(v); // I send the Value for the specified lineCode
                                            tv.put(t,v);
                                        }
                                    }
                                }

                            }
                        }

                    //}
                //}


        }catch (IOException e) {
            e.printStackTrace();
        }


    }


    public void notifyFailure(ServerSocket providerSocket, Socket connection, ObjectOutputStream out) {



        try {
            out.writeObject("Publisher node not responding!!!");
            out.flush();
        }catch (IOException e) {
            e.printStackTrace();
        }


    }


    public void init(int i) { // i= 1 or 2

        int lineNumber = 1;
        StringBuilder text = new StringBuilder();

        try{
            File f1 = new File("C:\\Users\\Owner\\AndroidStudioProjects\\src\\busLinesNew.txt");
            BufferedReader br1 = new BufferedReader(new FileReader(f1));
            String line;
            String[] myLine;
            if(i == 1) {
                while(lineNumber<=10) {

                    line = br1.readLine();
                    text.append(line + "\n");
                    myLine = line.split(",");
                    System.out.println("myLine " + myLine[0]);
                    Topic t = new Topic(myLine[0],myLine[1],myLine[2]);
                    topicsA.add(t);

                    lineNumber++;
                }
                System.out.println("\n");
            }else if(i == 2) {
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

            //while (true) {
                connection = providerSocket.accept(); // wait for connection

                System.out.println("Hi");

                ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(connection.getInputStream());


                out.writeObject("Publisher successfully connected to Broker.");
                out.flush();
                push(message,providerSocket,connection,out,this.getPublisherId()); // message is the lineCode

                message = (String) in.readObject();
                System.out.println(connection.getInetAddress().getHostAddress() + ">" + message);

                //System.out.println(in.readUTF());
                //System.out.println((Message) in.readObject());

                do {
                    try {



                        message = (String) in.readObject();
                        System.out.println(connection.getInetAddress().getHostAddress() + ">" + message);
                        //Thread.sleep(10000); // 10 sec
                    } catch (ClassNotFoundException classnot) {
                        System.err.println("Data received in unknown format");
                    }
                } while (!message.equals("bye"));


                in.close();
                out.close();
                connection.close();
            //} // while(true)

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


    public void disconect(Socket requestSocket, ObjectInputStream in, ObjectOutputStream out) {
        try {
            in.close();
            out.close();
            requestSocket.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }


    public void updateNodes() {

    }


    public static void main(String[] args) {
       //new PublisherMain1().openServer();
        int pubsNum = Integer.parseInt(args[0]);
        if(pubsNum==0) {
            System.out.println("Error! Cannot have 0 publishers.");
        }else {
            ArrayList<PublisherMain1> pubs = new ArrayList<>();
            for(int i=0;i<pubsNum;i++) {
                pubs.add(new PublisherMain1(i+1));

            }
            for(PublisherMain1 p: pubs) {
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
}
