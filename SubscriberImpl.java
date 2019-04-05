package DS_as1;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class SubscriberImpl implements Subscriber {
    private ArrayList<Info> getLines;

    private static Socket requestSocket = null;

    public static void main(String[] args) {
        new SubscriberImpl().connect();
    }

    //TODO: thn afhnw etsi pros to paron
    @Override
    public void init(int x) {

    }

    @Override
    public void connect() {
        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        String message;
        // Info line;
        try {
            requestSocket = new Socket(InetAddress.getByName("192.168.1.11"), 1 + 1000);
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            try {
                message = (String) in.readObject();
                System.out.println("Broker > " + message);
//TODO: na balw 821 gia na einai sta8ero h na soy dinw apo eisodo gia pio meta
                out.writeObject("THELW NA PARW TIN LISTA");
                out.flush();
                // Sullogh upeuthinon kleidion
                getLines = (ArrayList<Info>) in.readObject();

                out.writeObject("Bye o/ ");
                out.flush();


            } catch (ClassNotFoundException classNot) {
                System.out.println("Data received in unknown format.");
            }
        } catch (UnknownHostException unknownHost) {
            System.err.println("You are trying to connect to an unknown host!");
        } catch (IOException ioException) {
            ioException.printStackTrace();
            //TODO: fugei to finally kai paei sto disconnect, gia na ginetai sto telos
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

    public void register(Topic topic) {
        String IP, port;
        Info broker = new Info();
        //TODO:need taksinomisi
        for (broker : getLines) {
            if (broker.rensposibleLine > topic.key) {
                IP = broker.getIP();
                port = broker.getPort();
            }
        }
        //TODO: *
        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        BusInfo message;
        // Info line;
        //TODO: * isws na mpoyn se sxolia epeidh den xreiazetai na sundeomai sto socket
        try {
            requestSocket = new Socket(InetAddress.getByName(IP), Integer.parseInt(port));
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());
            try {
             //   message = (String) in.readObject();
             //   System.out.println("Broker > " + message);

            //    out.writeObject("Bye o/ ");
             //   out.flush();
                out.writeObject("THELW NA KANW REGISTER GIA TIN GRAMMI TADE");
                out.flush();
            message =(BusInfo) in.readObject();
    //TODO: ti na kanw me to minima / isws h BusInfo na mhn xreiazetai 8a to doyme
            } catch (ClassNotFoundException classNot) {
                System.out.println("Data received in unknown format.");
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


    public void disconnect(Broker brok, Topic topic) {

    }

    public void visualiseData(Topic topic, Value val) {
        System.out.println("");

    }
}
