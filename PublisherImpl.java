package DS_as1;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PublisherImpl implements Publisher
{
    public static void main(String[] args)
    {
        new PublisherImpl().connect();
    }

    public void connect()
    {
        ServerSocket providerSocket = null;
        Socket connection = null;
        String message = null;
        try {
            providerSocket = new ServerSocket(4321);

            while (true) {
                connection = providerSocket.accept();
                ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(connection.getInputStream());

                out.writeObject("Server successfully connected to Broker. ");
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
                System.out.println(brokers.size());
                for(BrokerImpl br : brokers)
                {
                    int topic = 100;
                    int noBrokers = brokers.size();
                    int start = 0;
                    int portion = (topic / noBrokers) + start;
                    System.out.println(br.brokerID + " pairnei " + start + "-" + portion);
                    start += portion;
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
}
