package DS_as1;

import java.io.*;
import java.net.*;

public class example1_Server {

    public static void main(String[] args) {
        new example1_Server().openServer();
    }

    public void openServer() {
        ServerSocket providerSocket = null;
        Socket connection = null;
        String message = null;
        try {
            /*******/

            providerSocket = new ServerSocket(4321);

            while (true) {
                /*
                 */
                connection = providerSocket.accept();
                ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(connection.getInputStream());

                out.writeObject("Server successfully connected to Broker. ");
                out.flush();
                do {
                    try {
                        /*
                         */
                        message = (String) in.readObject();
                        System.out.println(connection.getInetAddress().getHostAddress() + ">" + message);

                    } catch (ClassNotFoundException classnot) {
                        System.err.println("Data received in unknown format");
                    }
                } while (/***/!message.equals("bye"));
                in.close();
                out.close();
                connection.close();
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
