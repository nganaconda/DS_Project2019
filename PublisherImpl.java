

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;


public class PublisherImpl extends Thread implements Publisher {
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

    private static ArrayList<PublisherImpl> pubs = new ArrayList<>();

    public PublisherImpl(String ipnew, int portnew, int idnew) {
        ip = ipnew;
        port = portnew;
        id = idnew;
    }

    public ArrayList<Topic> getTopics() {
        return topics;
    }

    public ArrayList<Bus> getBuses() {
        return buses;
    }

    public ArrayList<Value> getValues() {
        return values;
    }

    public void run(){
        //this.connect();
        this.init(id);
    }

    public static void main(String[] args) {
        //new PublisherImpl("192.168.1.6", 4321).connect();
        //int numOfPubs = Integer.parseInt(args[0]);
        int numOfPubs = 6;
        if(numOfPubs == 0){
            System.out.println("You have chosen to run no Publishers. ");
        }
        else{
            for(int i = 1; i <= numOfPubs; i++) {
                pubs.add(new PublisherImpl("192.168.56.1", 4321+i, i)); // pubs.size = numOfPubs
            }
            for(PublisherImpl p: pubs) { // 1, 2, 3, 4
                p.start();
            }

        }
    }



    @Override
    public void init(int x) { // x is the id of a Publisher

        int numberOfLines = 0;
        try{
            File f1 = new File("C:\\Users\\Owner\\AndroidStudioProjects\\src\\busLinesNew.txt");
            BufferedReader br1 = new BufferedReader(new FileReader(f1));
            StringBuilder text = new StringBuilder();
            String line;
            while((line=br1.readLine())!=null) {
                text.append("\n");
                numberOfLines++;
            }


        }catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(numberOfLines);

        //int numberOfLines = 20; // I know it after I read All the text

        int i = numberOfLines/pubs.size(); // i: determines how many lines the publisher will read
        int mod = numberOfLines % pubs.size(); // mod: the additional lines that the last publisher will read
        // Last Publisher will get i + mod

        int portionStart = (x - 1)*i + 1; // portionStart: the begin of where to start to read

        int portionEnd;

        System.out.println("i: " + i);
        System.out.println("mod: " + mod);
        System.out.println("portionStart: " + portionStart);


        // If pubs=2 -> 1-10,11-20
        // If pubs=3 -> 1-6,7-12,13-20
        // If pubs=4 -> 1-5,6-10,11-15,16-20
        // if pubs=5 -> 1-4,5-8,9-12,13-16,17-20
        // if pubs=6 -> 1-3,4-6,7-9,10-12,13-15,16-20


        if(x == pubs.size()) {
            portionEnd =  portionStart + i - 1 + mod;
        }else {
            portionEnd = portionStart + i - 1;
        }
        System.out.println("portionEnd: " + portionEnd);



        int lineNumber = 1;
        try {
            File f1 = new File("C:\\Users\\Owner\\AndroidStudioProjects\\src\\busLinesNew.txt");
            BufferedReader br1 = new BufferedReader(new FileReader(f1));
            StringBuilder text1 = new StringBuilder();

            String line;
            String[] myLine;

            while((line=br1.readLine()) != null) {
                text1.append(line + "\n");

                //System.out.println("I ma in");
                while(lineNumber>=portionStart && lineNumber<=portionEnd) {
                    myLine = line.split(",");
                    Topic t = new Topic(myLine[0],myLine[1],myLine[2]);
                    pubs.get(x-1).getTopics().add(t);
                    System.out.println("I ma in 2");

                    line = br1.readLine();
                    text1.append(line + "\n");
                    lineNumber++;
                }

                lineNumber++;

            }
            //System.out.println(text1);
            System.out.println(pubs.get(x-1).getTopics().size());


//            while(lineNumber<=portionStart && lineNumber>=portionEnd) {
//                    line = br1.readLine();
//
//            }
        }catch (IOException e) {
            e.printStackTrace();
        }


//        for(int j = portionStart;j <= portionEnd;j++) {
//            // this for reads lines
//        }










        /*
        if(pubs.size()>=11){
            System.out.println("Publishers are way too many so I go to sleep...");
        }else {

            try{
               if(pubs.size()==2) {
                   File f1 = new File("C:\\Users\\Owner\\AndroidStudioProjects\\src\\busLinesNew.txt");
                   BufferedReader br1 = new BufferedReader(new FileReader(f1));  // 1st Publisher - Topics
                   StringBuilder text1 = new StringBuilder();
                   int lineNumber = 1;
                   String line;
                   String[] myLine;
                   while(lineNumber<=10) {
                       line = br1.readLine();
                       text1.append(line + "\n");
                       myLine = line.split(",");
                       //System.out.println("myLine " + myLine[0]);
                       Topic t = new Topic(myLine[0],myLine[1],myLine[2]);
                       if(x==0) {
                           pubs.get(0).getTopics().add(t);
                       }
                       lineNumber++;
                   }

                   File f2 = new File("C:\\Users\\Owner\\AndroidStudioProjects\\src\\busPositionsNew.txt");
                   BufferedReader br2 = new BufferedReader(new FileReader(f2));  // 1st Publisher - Values
                   StringBuilder text2 = new StringBuilder();

                   while((line=br2.readLine()) != null) {
                       text2.append(line + "\n");
                       myLine = line.split(",");
                       Bus b = new Bus(myLine[0],myLine[1],myLine[2],myLine[5]);
                       Value v = new Value(b,Double.parseDouble(myLine[3]),Double.parseDouble(myLine[4]));
                       if(x==0) {
                           pubs.get(0).getValues().add(v);
                       }
                   }







                   br1.close();  // 2nd Publisher - Topics
                   text1 = null;
                   lineNumber = 1;
                   while(lineNumber<=10) {  // Do Nothing
                       line = br1.readLine();
                       text1.append(line + "\n");

                       lineNumber++;
                   }
                   while((line=br1.readLine()) != null) {
                       text1.append(line + "\n");
                       myLine = line.split(",");
                       if(lineNumber==11) {
                           Topic t = new Topic(myLine[0],myLine[1],myLine[2]);
                           if(x==1) {
                               pubs.get(1).getTopics().add(t);
                           }

                       }
                       while(lineNumber>=11 && (line=br1.readLine()) != null) {
                           text1.append(line + "\n");
                           myLine = line.split(",");
                           Topic t = new Topic(myLine[0],myLine[1],myLine[2]);
                           if(x==1) {
                               pubs.get(1).getTopics().add(t);
                           }

                           lineNumber++;
                       }

                       lineNumber++;
                   }

                   br2.close(); // 2nd Publisher - Values
                   text2 = null;
                   while((line=br2.readLine()) != null) {
                       text2.append(line + "\n");
                       myLine = line.split(",");
                       Bus b = new Bus(myLine[0],myLine[1],myLine[2],myLine[5]);
                       Value v = new Value(b,Double.parseDouble(myLine[3]),Double.parseDouble(myLine[4]));
                       if(x==1) {
                           pubs.get(1).getValues().add(v);
                       }
                   }


               }else if(pubs.size()==3) {
                   File f1 = new File("C:\\Users\\Owner\\AndroidStudioProjects\\src\\busLinesNew.txt");
                   BufferedReader br1 = new BufferedReader(new FileReader(f1));  // 1st Publisher - Topics
                   StringBuilder text1 = new StringBuilder();
                   int lineNumber = 1;
                   String line;
                   String[] myLine;


                   File f2 = new File("C:\\Users\\Owner\\AndroidStudioProjects\\src\\busPositionsNew.txt");
                   BufferedReader br2 = new BufferedReader(new FileReader(f2));  // 1st Publisher - Values
                   StringBuilder text2 = new StringBuilder();

                   while((line=br2.readLine()) != null) {
                       text2.append(line + "\n");
                       myLine = line.split(",");
                       Bus b = new Bus(myLine[0],myLine[1],myLine[2],myLine[5]);
                       Value v = new Value(b,Double.parseDouble(myLine[3]),Double.parseDouble(myLine[4]));
                       if(x==0) {
                           pubs.get(0).getValues().add(v);
                       }
                   }

                   br1.close();  // 2nd Publisher - Topics
                   text1 = null;
                   while(lineNumber<=6) { // Fill the Topics of the 1st Publisher
                       line = br1.readLine();
                       text1.append(line + "\n");
                       myLine = line.split(",");
                       Topic t = new Topic(myLine[0],myLine[1],myLine[2]);
                       if(x==0) {
                           pubs.get(0).getTopics().add(t);
                       }
                       lineNumber++;
                   }
                   while(lineNumber>=7 && lineNumber<=13) {
                       line = br1.readLine();
                       text1.append(line + "\n");
                       myLine = line.split(",");
                       if(lineNumber==7) {
                           Topic t = new Topic(myLine[0],myLine[1],myLine[2]);
                           if(x==1) {
                               pubs.get(1).getTopics().add(t);
                           }
                       }
                       while(lineNumber>=7 && lineNumber<=13 && (line=br1.readLine()) != null) {
                           text1.append(line + "\n");
                           myLine = line.split(",");
                           Topic t = new Topic(myLine[0],myLine[1],myLine[2]);
                           if(x==1) {
                               pubs.get(1).getTopics().add(t);
                           }
                           lineNumber++;
                       }
                       lineNumber++;
                   }
                   while(lineNumber>=14 && lineNumber<=20) {
                       line = br1.readLine();
                       text1.append(line + "\n");
                       myLine = line.split(",");
                       if(lineNumber==14) {
                           Topic t = new Topic(myLine[0],myLine[1],myLine[2]);
                           if(x==1) {
                               pubs.get(2).getTopics().add(t);
                           }
                       }
                       while( lineNumber>=14 && lineNumber<=20 && (line=br1.readLine()) != null) {
                           text1.append(line + "\n");
                           myLine = line.split(",");
                           Topic t = new Topic(myLine[0],myLine[1],myLine[2]);
                           if(x==1) {
                               pubs.get(2).getTopics().add(t);
                           }
                           lineNumber++;
                       }
                       lineNumber++;
                   }
                   br2.close();
                   text2 = null;
                   while((line=br2.readLine()) != null) {
                       text2.append(line + "\n");
                       myLine = line.split(",");
                       Bus b = new Bus(myLine[0],myLine[1],myLine[2],myLine[5]);
                       Value v = new Value(b,Double.parseDouble(myLine[3]),Double.parseDouble(myLine[4]));
                       if(x==0) {
                           pubs.get(1).getValues().add(v);
                       }
                   }




               }else if(pubs.size()==4) {



               }



            }catch (IOException e) {
                e.printStackTrace();
            }


        }
        */



    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }


    public void connect() {

    }

    /*
        public void connect() {
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
                        }
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
                        }
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
                        String newtopic = (String) in.readObject();
                        String reply = "821,1804,10015,37.985427,23.75514,Mar  4 2019 10:39:00:000AM";
                        out.writeObject(reply);

                        in.close();
                        out.close();
                        connection.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (IOException ioException) {
                ioException.printStackTrace();
            } finally {
                try {
                    providerSocket.close();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }

        }

        */

    public void push(String busLineId) {

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

    public void disconnect(Socket requestSocket) {
        try {
            in.close();
            out.close();
            requestSocket.close();

        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }


    }
