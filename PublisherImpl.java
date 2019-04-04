package DS_as1;

import com.sun.security.ntlm.Server;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PublisherImpl implements Publisher
{
    public int port;
    public String ip;
    private static ServerSocket providerSocket = null;

    public PublisherImpl(String ipnew, int portnew)
    {
        ip = ipnew;
        port = portnew;
    }


    public static void main(String[] args)
    {
        new PublisherImpl("192.168.1.7", 4321).connect();
    }

    @Override
    public void init(int x) {

    }

    public void connect()
    {
        Socket connection = null;
        String message = null;
        try {
            providerSocket = new ServerSocket(port);

            while(true) {
                connection = providerSocket.accept();
                ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(connection.getInputStream());

                out.writeObject("Server successfully connected to Broker. ");
                out.flush();
                do {
                    try {
                        message = (String) in.readObject();
                        if(message.equals("bye")){
                            break;
                        }
                        System.out.println(connection.getInetAddress().getHostAddress() + "> " + message);
                        int id = Integer.parseInt(message.split(" ")[0]);
                        String ipB = message.split(" ")[1];
                        String portB = message.split(" ")[2];
                        Broker b = new BrokerImpl1(ipB, Integer.parseInt(portB));

                        String topic = (String) in.readObject();
                        ArrayList<Topic> top = new ArrayList<Topic>();
                        for(int i = 0; i < topic.split(" ").length; i++){
                            top.add(new Topic(topic.split(" ")[i]));
                        }
                        b.setTopics(top);
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
                } while (!message.equals("bye"));
                in.close();
                out.close();
                connection.close();
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
}
