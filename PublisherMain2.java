import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;


public class PublisherMain2 implements Publisher {

    private static ArrayList<Topic> topics = new ArrayList<>();
    private static ArrayList<Bus> buses = new ArrayList<>();
    private static ArrayList<Value> values = new ArrayList<>();

    @Override
    public void getBrokerList() {

    }

    @Override
    public void hashTopic(Topic t) {
        //return null;
    }

    @Override
    public void push(Topic t, Value v) {

    }

    @Override
    public void notifyFailure(Broker b) {

    }

    @Override
    public void init(int i) {

    }

    @Override
    public void connect() {

    }

    @Override
    public void disconect(Socket requestSocket, ObjectInputStream in, ObjectOutputStream out) {
        try {
            in.close();
            out.close();
            requestSocket.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    @Override
    public void updateNodes() {

    }



    public static void main(String[] args) {


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






            System.out.println("End of PublisherMain2....");
            br1.close();
            br2.close();
        }catch (IOException e) {
            e.printStackTrace();
        }



    }


    public void openServer() { // Sends data and messages to brokers.
        ServerSocket providerSocket = null;
        Socket connection = null;
        String message = null;
        try {
            providerSocket = new ServerSocket(4321);

            //while (true) {
            connection = providerSocket.accept();
            ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(connection.getInputStream());

            System.out.println(in.readUTF());
            System.out.println((Message) in.readObject());










            in.close();
            out.close();
            connection.close();
            //Thread.sleep(4224);
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





}
