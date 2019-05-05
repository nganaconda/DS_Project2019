package DS_as1;


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
    private static ArrayList<Value> values = new ArrayList<>();
    private ArrayList<Topic> topics = new ArrayList<>();
    //private static ArrayList<PublisherImpl> publishers = new ArrayList<PublisherImpl>();

    public PublisherImpl(String ipnew, int portnew, int idnew) {
        ip = ipnew;
        port = portnew;
        id = idnew;
    }

    public void run(){
        //System.out.println("Current time: " + System.currentTimeMillis());
        Thread t1 = new Thread(){
            public void run(){
                PublisherImpl.this.init(id);
            }
        };
        t1.start();
        this.connect();
    }


    public static void main(String[] args) {

//        int numOfPubs = Integer.parseInt(args[0]);
//        if(numOfPubs == 0){
//            System.out.println("You have chosen to run no Publishers. ");
//        }else if(numOfPubs>=21) {
//            System.out.println("Too many publishers. ");
//        }
//        else{
//            for(int i = 1; i <= numOfPubs; i++) {
//                publishers.add(new PublisherImpl("192.168.1.5", 4321+i-1, i));
//            }
//            for(PublisherImpl p : publishers){
//                p.start();
//            }
//        }

        int id = Integer.parseInt(args[0]);
        PublisherImpl p = (PublisherImpl) publishers.get(id);
        p.start();
    }

    @Override
    public void init(int x) {
        int numberOfLines = 0;
        try{
            File f1 = new File("src/busLinesNew.txt");
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

        int i = numberOfLines/publishers.size();
        int mod = numberOfLines % publishers.size();

        int portionStart = x*i + 1;

        int portionEnd;
        if(x == publishers.size()) {
            portionEnd =  portionStart + i - 1 + mod;
        }else {
            portionEnd = portionStart + i - 1;
        }
        System.out.println("portionStart: " + portionStart);
        System.out.println("portionEnd: " + portionEnd);



        int lineNumber = 1;
        try {
            File f1 = new File("src/busLinesNew.txt");
            BufferedReader br1 = new BufferedReader(new FileReader(f1));
            StringBuilder text1 = new StringBuilder();

            String line;
            String[] myLine;

            while((line=br1.readLine()) != null) {
                text1.append(line + "\n");

                while(lineNumber>=portionStart && lineNumber<=portionEnd) {
                    myLine = line.split(",");
                    Topic t = new Topic(myLine[0],myLine[1],myLine[2]);
                    publishers.get(x).getTopics().add(t);

                    line = br1.readLine();
                    text1.append(line + "\n");
                    lineNumber++;
                }
                lineNumber++;
            }

            File f2 = new File("src/busPositionsNew.txt");
            BufferedReader br2 = new BufferedReader(new FileReader(f2));
            StringBuilder text2 = new StringBuilder();

            while((line = br2.readLine()) != null) {
                for(int j=0;j < 100;j++) {
                    if(line!=null) {
                        text2.append(line + "\n");
                        myLine = line.split(",");
                        Bus b = new Bus(myLine[0],myLine[1],myLine[2],myLine[5]);
                        publishers.get(x).getBuses().add(b);
                        Value v = new Value(b,Double.parseDouble(myLine[3]),Double.parseDouble(myLine[4]));
                        publishers.get(x).getValues().add(v);

                        //System.out.println("myLine is: " + myLine[5]); EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE
                        line = br2.readLine();
                    }else {
                        break;
                    }
                }
                System.out.println("Publisher id is: " + id );
                try {

                    this.sleep(5000);
                }catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }



        } catch (IOException e) {
            e.printStackTrace();
        }

    }

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

                        message = (String) in.readObject();

                        System.out.println(connection.getInetAddress().getHostAddress() + "> " + message);
                        int id = Integer.parseInt(message.split(" ")[0]);
                        String ipB = message.split(" ")[1];
                        String portB = message.split(" ")[2];
                        Broker b = new BrokerImpl1(id, ipB, Integer.parseInt(portB));
                        b.setRequestSocket(connection);

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
                    try {
                        connection = providerSocket.accept();
                        out = new ObjectOutputStream(connection.getOutputStream());
                        in = new ObjectInputStream(connection.getInputStream());
                        Topic newtopic = (Topic) in.readObject();
                        push(newtopic.getBusLineId());


                    } catch (Exception e) {
                        //notifyFailure();
                        e.printStackTrace();
                    }
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }

    }

    public void push(String busLineId) {
        Tuple<Value> tupleList = new Tuple<>();
        String lineCode = null;

        long crtD = System.currentTimeMillis()/1000;
        System.out.println("Current time before initializing the search for the busLineId: " + busLineId +  " "  + crtD);

        for(int i = 0; i < this.topics.size(); i++) { // Checks the busLines the pub is responsible for
            if(busLineId.equals(this.topics.get(i).getBusLineId())) {
                lineCode = this.topics.get(i).getLineCode();
                boolean emptyValues = true;

                while(emptyValues) {
                    if(!this.values.isEmpty()) { // values list refreshes every 5sec
                        for (int j = 0; j < this.values.size(); j++) { // Every 5sec this for gets bigger
                            if(this.values.get(j) != null) { // unnecessary if
                                if (lineCode.equals(this.values.get(j).getBus().getLineCode())) {
                                    tupleList.add(this.values.get(j));
                                }
                            }

                        }

                        if(!tupleList.isEmpty()) {
                            emptyValues = false;
                        }
                    }
                    System.out.println("I am still searching....");
                }
            }
        }
        System.out.println("Current time FINAL initializing the search for the busLine: " + busLineId +  " "  + System.currentTimeMillis()/1000);
        System.out.println("Elapsed time: " + ((System.currentTimeMillis()/1000)-crtD) + " seconds.");
        if(!tupleList.isEmpty()) {
            System.out.println("TupleList size is: " + tupleList.size());
            try{
                out.writeObject("Yes");
                out.writeObject(tupleList);
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
        else{
            try{
                out.writeObject("No");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public ArrayList<Topic> getTopics() {
        return topics;
    }

    public void setTopics(ArrayList<Topic> topics) {
        this.topics = topics;
    }

    public ArrayList<Value> getValues() {
        return values;
    }

    public void setValues(ArrayList<Value> values) {
        this.values = values;
    }

    public ArrayList<Bus> getBuses() {
        return buses;
    }

    public void setBuses(ArrayList<Bus> buses) {
        this.buses = buses;
    }



    public void notifyFailure() {
        try {
            out.writeObject("Publisher node not responding!!!");
            out.flush();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void disconnect() {
        try {
            in.close();
            out.close();
            providerSocket.close();

        } catch (IOException ioException) {
            ioException.printStackTrace();
        }

    }



}
