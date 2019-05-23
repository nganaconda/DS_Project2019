package DS_as1;

import jdk.internal.cmm.SystemResourcePressureImpl;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class PublisherImpl1 extends Thread implements Publisher {
    private int port;
    public String ip;
    private int id;
    private int numberOfPubs = 1;
    private Socket providerSocket = null;
    public Socket socket = null;
    ObjectOutputStream out;
    ObjectInputStream in;
    private ArrayList<Bus> buses = new ArrayList<>();
    private static ArrayList<Value> values = new ArrayList<>();
    private ArrayList<Topic> topics = new ArrayList<>();

    public PublisherImpl1(int idnew) {
        id = idnew;
    }

    public PublisherImpl1(){
        id = 0;
    }

    public int getID(){
        return id;
    }

    public int getPort(){
        return port;
    }

    public String getIP(){
        return ip;
    }

    public void run(){
        this.connect();
        Thread t1 = new Thread(){
            public void run(){
                init(id);
            }
        };
        t1.start();
    }


    public static void main(String[] args) {
        PublisherImpl pub = new PublisherImpl();
        pub.start();
    }

    @Override
    public void init(int x) {
        this.prepare(x);

        int numOfPubs = numberOfPubs;
        String line;
        String[] myLine;

        File f2 = new File("DS_project_dataset/busPositionsNew.txt");
        BufferedReader br2 = null;
        try {
            br2 = new BufferedReader(new FileReader(f2));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        StringBuilder text2 = new StringBuilder();

        while(true) {
            try{
                if ((line = br2.readLine()) != null) {
                    if(numOfPubs != numberOfPubs){
                        numOfPubs = numberOfPubs;
                        this.prepare(x);
                    }
                    text2.append(line + "\n");
                    myLine = line.split(",");

                    for (Topic t : topics) {
                        if (t.getLineCode().equals(myLine[0])) {
                            Bus b = new Bus(myLine[0], myLine[1], myLine[2], myLine[5]);
                            b.setLineName(t.getBusLineId());
                            /*boolean newbus = true;
                            ArrayList<Bus> buss = buses;
                            for (Bus bus : buss) {
                                if (bus.getVehicleId().equals(b.getVehicleId()) && bus.getRouteCode().equals(b.getRouteCode())) {
                                    buses.set(buses.indexOf(bus), b);
                                    newbus = false;
                                }
                            }
                            if (newbus == true) {
                                buses.add(b);
                            }*/
                            Value v = new Value(b, Double.parseDouble(myLine[3]), Double.parseDouble(myLine[4]));
                            /*if (newbus == true) {
                                values.add(v);
                            } else {
                                ArrayList<Value> vals = values;
                                for (Value val : vals) {
                                    if (val.getBus().getVehicleId().equals(v.getBus().getVehicleId()) && val.getBus().getRouteCode().equals(v.getBus().getRouteCode())) {
                                        values.set(values.indexOf(val), v);
                                    }
                                }
                            }*/
                            try {
                                for(Broker br : brokers) {
                                    BrokerImpl bro = (BrokerImpl) br;
                                    if(bro.connection != null) {
                                        providerSocket = new Socket(br.getIp(), br.getPort() + 1);
                                        out = new ObjectOutputStream(providerSocket.getOutputStream());
                                        in = new ObjectInputStream(providerSocket.getInputStream());
                                        push(v);
                                    }
                                }

                            } catch (Exception e) {
                            }
                            try {
                                this.sleep(50);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
                else{
                    break;
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void prepare(int x){
        int numberOfLines = 0;      //8a xreiastei gia na xwrisoume ta topics anamesa stous posous pubs einai sundedemenoi

        //metrame tis grammes tou busLinesNew dld posa einai sunolika ta topics
        try {
            File f1 = new File("DS_project_dataset/busLinesNew.txt");
            BufferedReader br1 = new BufferedReader(new FileReader(f1));
            StringBuilder text = new StringBuilder();
            String line;
            while ((line = br1.readLine()) != null) {
                text.append("\n");
                numberOfLines++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        int i = numberOfLines / numberOfPubs;         //to meridio tou ka8e pub
        int mod = numberOfLines % numberOfPubs;     //posa topics 8a perisse4oun an ta topics den diairountai akrivws stous pubs

        int portionStart = x * i + 1;                 //apo poia grammh kai meta einai ta topics gia ta opoia einai upeu8unos o ka8e pub

        int portionEnd;                             //se poia grammh einai to teleutaio topic gia to opoio einai upeu8unos o ka8e pub
        if (x == numberOfPubs) {
            portionEnd = portionStart + i - 1 + mod;
        } else {
            portionEnd = portionStart + i - 1;
        }
        System.out.println("portionStart: " + portionStart);
        System.out.println("portionEnd: " + portionEnd);


        //topo8etoume sth lista me ta topics auta gia ta opoia einai upeu8unos o trexwn pub
        int lineNumber = 1;
        try {
            File f1 = new File("DS_project_dataset/busLinesNew.txt");
            BufferedReader br1 = new BufferedReader(new FileReader(f1));
            StringBuilder text1 = new StringBuilder();

            String line;
            String[] myLine;
            topics.clear();

            while ((line = br1.readLine()) != null) {
                text1.append(line + "\n");

                while (lineNumber >= portionStart && lineNumber <= portionEnd) {
                    myLine = line.split(",");
                    Topic t = new Topic(myLine[0], myLine[1], myLine[2]);
                    topics.add(t);

                    line = br1.readLine();
                    text1.append(line + "\n");
                    lineNumber++;
                }
                lineNumber++;
            }
            System.out.println("This publisher is responsible for these line ids : line codes.");
            for(Topic top : topics){
                System.out.print(top.getBusLineId() + ":" + top.getLineCode() + " ");
            }
            System.out.println("\n...................................................................................................................................................................");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void connect() {
        String message;
        for(Broker bro : brokers) {
            try {
                providerSocket = new Socket(bro.getIp(), bro.getPort()+1);

                try {
                    out = new ObjectOutputStream(providerSocket.getOutputStream());
                    in = new ObjectInputStream(providerSocket.getInputStream());

                    out.writeObject("Hi!");

                    id = (int) in.readObject();
                    numberOfPubs = (int) in.readObject();
                    System.out.println("ID " + id);
                    System.out.println("numOfPubs " + numberOfPubs);

                    out.writeObject("Server " + this.id + " successfully connected to Broker. ");
                    out.flush();

                    message = (String) in.readObject();

                    System.out.println(bro.getIp() + "> " + message);
                    int idB = Integer.parseInt(message.split(" ")[0]);
                    String ipB = message.split(" ")[1];
                    String portB = message.split(" ")[2];
                    BrokerImpl b = new BrokerImpl(idB, ipB, Integer.parseInt(portB));

                    Info<Topic> topic = (Info<Topic>) in.readObject();
                    ArrayList<Topic> top = topic;
                    b.setTopics(top);

                    System.out.println("Broker " + b.getPort() + ":");
                    for (Topic t : b.getTopics()) {
                        System.out.print(t.getBusLineId() + " ");
                    }
                    System.out.println("\n");

                    b.connection = providerSocket;
                    brokers.set(idB, b);

                } catch (ClassNotFoundException classnot) {
                    System.err.println("Data received in unknown format");
                }
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    public void push(Value value) {
        try{
            out.writeObject(" ");
            out.flush();

            int num = (int) in.readObject();
            if(num != numberOfPubs){
                numberOfPubs = num;
                System.out.println("NumOfPubs " + numberOfPubs);
            }

            out.writeObject(value);
            out.flush();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
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
