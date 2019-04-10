import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public interface Publisher extends Node {

    //public void getBrokerList();

    //public void hashTopic(Topic t);

    public void push(String busLineId);  // Topic t, Value v

    public void notifyFailure();

    public String getIp();

    public int getPort();

    //public Socket getSocket();

}
