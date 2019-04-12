package DS_as1;

import com.sun.security.ntlm.Server;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PublisherImpl extends Thread implements Publisher
{
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
    private static ArrayList<PublisherImpl> publishers = new ArrayList<PublisherImpl>();

    public PublisherImpl(String ipnew, int portnew, int idnew)
    {
        ip = ipnew;
        port = portnew;
        id = idnew;
    }

    public void run(){
        this.init(id);
        System.out.println("\n" + id + ": ");
        for(Topic t : this.getTopics()){
            System.out.print(t.getBusLineId());
            System.out.print(" ");
        }
        System.out.println("\n");
        this.connect();
    }


    public static void main(String[] args)
    {
        int numOfPubs = Integer.parseInt(args[0]);
        if(numOfPubs == 0){
            System.out.println("You have chosen to run no Publishers. ");
        }
        else{
            for(int i = 1; i <= numOfPubs; i++) {
                publishers.add(new PublisherImpl("192.168.1.6", 4321+i-1, i));
            }
            for(PublisherImpl p : publishers){
                p.start();
            }
        }
    }

    @Override
    public void init(int x) {
        int numberOfLines = 0;
        try{
            File f1 = new File("DS_project_dataset/busLinesNew.txt");
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

        int portionStart = (x - 1)*i + 1;

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
            File f1 = new File("DS_project_dataset/busLinesNew.txt");
            BufferedReader br1 = new BufferedReader(new FileReader(f1));
            StringBuilder text1 = new StringBuilder();

            String line;
            String[] myLine;

            while((line=br1.readLine()) != null) {
                text1.append(line + "\n");

                while(lineNumber>=portionStart && lineNumber<=portionEnd) {
                    myLine = line.split(",");
                    Topic t = new Topic(myLine[0],myLine[1],myLine[2]);
                    publishers.get(x-1).getTopics().add(t);

                    line = br1.readLine();
                    text1.append(line + "\n");
                    lineNumber++;
                }
                lineNumber++;
            }

            File f2 = new File("DS_project_dataset/busPositionsNew.txt");
            BufferedReader br2 = new BufferedReader(new FileReader(f2));
            StringBuilder text2 = new StringBuilder();

            while((line = br2.readLine()) != null) {
                text2.append(line + "\n");
                myLine = line.split(",");
                Bus b = new Bus(myLine[0],myLine[1],myLine[2],myLine[5]);
                publishers.get(x-1).getBuses().add(b);
                //buses.add(b);
                Value v = new Value(b, Double.parseDouble(myLine[3]), Double.parseDouble(myLine[4]));
                publishers.get(x-1).getValues().add(v);
                //values.add(v);
            }


        } catch (IOException e) {
            e.printStackTrace();
}



    }

    public void connect()
    {
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
                        }*/
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
                        }*/
                        Info<Topic> topic = (Info<Topic>) in.readObject();
                        ArrayList<Topic> top = topic;
                        b.setTopics(top);
                        b.setRequestSocket(connection);
                        brokers.set(id, b);
                        System.out.println("Broker " + b.getPort() + ":");
                        for(Topic t : b.getTopics())
                        {
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

                        /*in.close();
                        out.close();
                        connection.close();*/
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } /*finally {
                try {
                    providerSocket.close();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }*/

    }

    public void push(String busLineId) {
        Tuple<Value> tupleList = new Tuple<>();
        String lineCode = null;
        for(int i = 0; i < this.topics.size(); i++) {
            if(busLineId.equals(this.topics.get(i).getBusLineId())) {
                lineCode = this.topics.get(i).getLineCode();
            }
        }
        //System.out.println("Size of topics list is: " + pubs.get(id-1).getTopics().size());
        System.out.println(lineCode);
        //System.out.println("Size of values list is: " + pubs.get(id-1).getValues().size());
        if(lineCode != null) {
            for(int i=0; i < this.values.size(); i++) {
                //String lineCodeTemp = pubs.get(id-1).getValues().get(i).getBus().getLineCode();
                if(lineCode.equals(this.values.get(i).getBus().getLineCode())) {
                    tupleList.add(this.values.get(i));
                    //System.out.println(lineCodeTemp);
                }
            }
        }
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
}
