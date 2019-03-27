package DS_as1;
import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;


public class BrokerImpl extends Thread implements Broker {
    public static String brokerID;
    private static Socket requestSocket = null;
    private static ServerSocket replySocket = null;
    private static String hash;

    public BrokerImpl(String IDnew) {
        brokerID = IDnew;
    }

    public void run() {
        System.out.println("Thread " + brokerID + " started.");
    }

    public static void main(String[] args) {

    }

    public void connect() {
    }

    public void calculateKeys() {
        String port = Integer.toString(requestSocket.getLocalPort());
        String ip = requestSocket.getLocalAddress().toString();
        hash = Integer.toString((ip + "/" + port).hashCode());
    }

    public void acceptConnection(Publisher pub) {
        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        String publisher;
        try {
            requestSocket = new Socket(InetAddress.getByName("192.168.56.1"), 4321);
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            try {
                publisher = (String) in.readObject();
                System.out.println("Server > " + publisher);

                out.writeObject("Thread with id " + brokerID + " and hash " + hash);
                out.flush();

                out.writeObject("bye");
                out.flush();
            } catch (ClassNotFoundException classNot) {
                System.out.println("data received in unknown format");
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


    public void acceptConnection(Subscriber sub) {
        Socket connection = null;
        String message = null;
        try {
            replySocket = new ServerSocket(4324);
            while (true) {
                connection = replySocket.accept();
                ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(connection.getInputStream());

                out.writeObject("Broker successfully connected to Client. ");
                out.flush();
                do {
                    try {
                        message = (String) in.readObject();
                        System.out.println(connection.getInetAddress().getHostAddress() + "> " + message);

                    } catch (ClassNotFoundException classnot) {
                        System.err.println("Data received in unknown format");
                    }
                } while (!message.equals("bye"));
                in.close();
                out.close();
                connection.close();
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            try {
                replySocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }


    public void notifyPublisher(String msg) {
        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        try {
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());
            try {
                out.writeObject(msg);
                out.flush();
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}