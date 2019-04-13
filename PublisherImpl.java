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
    private ObjectOutputStream out;
    private ObjectInputStream in;
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
        System.out.println("\n" + id + ": ");
        for(Topic t : this.getTopics()){
            System.out.print(t.getBusLineId());
            System.out.print(" ");
        }
        System.out.println("\n");
        //this.push("060"); //ENABLE THIS ONLY IF THERE IS A CONNECTION WITH A BROKER
        //this.connect();
    }

    public static void main(String[] args) {
        //new PublisherImpl("192.168.1.6", 4321).connect();
        //int numOfPubs = Integer.parseInt(args[0]); // THIS IS THE REAL VARIABLE
        int numOfPubs = 2; // temp for testing
        if(numOfPubs == 0){
            System.out.println("You have chosen to run no Publishers. ");
        }
        else if(numOfPubs >= 21) {
            System.out.println("Too many publishers.");
        }
        else{
            for(int i = 1; i <= numOfPubs; i++) {
                pubs.add(new PublisherImpl("192.168.56.1", 4321+i, i)); // pubs.size = numOfPubs
            }
            for(PublisherImpl p: pubs) { // 1, 2, 3, 4
                p.start();
                //p.push("060");
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
                while(lineNumber>=portionStart && lineNumber<=portionEnd) {
                    myLine = line.split(",");
                    Topic t = new Topic(myLine[0],myLine[1],myLine[2]);
                    pubs.get(x-1).getTopics().add(t);
                    line = br1.readLine();
                    text1.append(line + "\n");
                    lineNumber++;
                }
                lineNumber++;
            }

            File f2 = new File("C:\\Users\\Owner\\AndroidStudioProjects\\src\\busPositionsNew.txt");
            BufferedReader br2 = new BufferedReader(new FileReader(f2));
            StringBuilder text2 = new StringBuilder();

            int lineNum = 0;
            while((line = br2.readLine()) != null) {
                for(int j=0;j < 50;j++) {
                    if(line!=null) {
                        text2.append(line + "\n");
                        myLine = line.split(",");
                        Bus b = new Bus(myLine[0],myLine[1],myLine[2],myLine[5]);
                        pubs.get(x-1).getBuses().add(b);
                        //buses.add(b);
                        Value v = new Value(b,Double.parseDouble(myLine[3]),Double.parseDouble(myLine[4]));
                        pubs.get(x-1).getValues().add(v);
                        //values.add(v);
                        System.out.println("myLine is: " + myLine[5]);


                        lineNum++;
                        line = br2.readLine();
                    }else {
                        break;
                    }
                }
                System.out.println("Publisher id is: " + id );
                try {
                    this.sleep(10000);
                }catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }




            //while((line = br2.readLine()) != null) {
                //text2.append(line + "\n");
                //myLine = line.split(",");
                //lineNum++;
                /*
                String[] timeStampOfBusPosition = myLine[5].split("  ");
                String[] dayAndTimeStamp = timeStampOfBusPosition[1].split(" ");
                if(dayAndTimeStamp[0].equals("4")) {
                    //System.out.println("dayAndTimeStamp[1] is: " + dayAndTimeStamp[0]);
                    String[] timeStamp = dayAndTimeStamp[2].split(":");
                    //System.out.println("timeStamp is " + timeStamp[0]);

                }else if(dayAndTimeStamp[0].equals("5")) {

                }else if(dayAndTimeStamp[0].equals("6")) {

                }else if(dayAndTimeStamp[0].equals("7")) {

                }
                */
                //System.out.println("dayAndTimeStamp[1] is: " + dayAndTimeStamp[2]);
                //System.out.println("timeStampOfBusPosition[0] is: " + timeStampOfBusPosition[0] + " timeStampOfBusPosition[1] is: " + timeStampOfBusPosition[1]);
            //}



            //System.out.println(text1);
            //System.out.println(pubs.get(x-1).getTopics().size());



        }catch (IOException e) {
            e.printStackTrace();
        }


    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }


//    public void connect() {
//
//    }


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
                    if(message.equals("bye")){
                        break;
                    }
                    System.out.println(connection.getInetAddress().getHostAddress() + "> " + message);
                    int id = Integer.parseInt(message.split(" ")[0]);
                    String ipB = message.split(" ")[1];
                    String portB = message.split(" ")[2];
                    Broker b = new BrokerImpl1(id, ipB, Integer.parseInt(portB));
                    b.setRequestSocket(connection);

                    //ousiastika enhmerwnoume th lista me tous brokers me ta topics gia ta opoia einai upeu8unos o ka8enas
                    //String topic = (String) in.readObject();
                    //ArrayList<Topic> top = new ArrayList<Topic>();
                    //for(int i = 0; i < topic.split(" ").length; i++){
                       // top.add(new Topic(topic.split(" ")[i]));
                    //}
                    Info<Topic> topic = (Info<Topic>) in.readObject();
                    ArrayList<Topic> top = topic;
                    b.setTopics(top);
                    b.setRequestSocket(connection);
                    brokers.set(id, b);
                    System.out.println("Broker " + b.getPort() + ":");
                    for(Topic t : b.getTopics()) {
                        System.out.print(t.getBusLineId() + " ");
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
                        Topic newtopic = (Topic) in.readObject();
                        push(newtopic.getBusLineId());

//                        in.close();
//                        out.close();
//                        connection.close();
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




    public void push(String busLineId) {
        Tuple<Value> tupleList = new Tuple<>();
        String lineCode = null;
        for(int i=0;i<pubs.get(id-1).getTopics().size();i++) {
            if(busLineId.equals(pubs.get(id-1).getTopics().get(i).getBusLineId())) {
                lineCode = pubs.get(id-1).getTopics().get(i).getLineCode();
            }
        }
        //System.out.println("Size of topics list is: " + pubs.get(id-1).getTopics().size());
        //System.out.println(lineCode);
        //System.out.println("Size of values list is: " + pubs.get(id-1).getValues().size());
        if(lineCode!=null) {
            for(int i=0;i<pubs.get(id-1).getValues().size();i++) {
                //String lineCodeTemp = pubs.get(id-1).getValues().get(i).getBus().getLineCode();
                if(lineCode.equals(pubs.get(id-1).getValues().get(i).getBus().getLineCode())) {
                    tupleList.add(pubs.get(id-1).getValues().get(i));
                    //System.out.println(lineCodeTemp);

                }
            }
        }
//        if(!tupleList.isEmpty()) {
//            System.out.println("TupleList size is: " + tupleList.size());
//            try{
//                out.writeObject(tupleList);
//            }catch (IOException e) {
//                e.printStackTrace();
//            }
//        }

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
