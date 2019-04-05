package DS_as1;

import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public interface Publisher extends Node {

    public void getBrokerList();

    public void hashTopic(Topic t);

    public void push(String lineCode, ServerSocket providerSocket, Socket connection, ObjectOutputStream out,int j);  // Topic t, Value v

    public void notifyFailure(ServerSocket providerSocket, Socket connection, ObjectOutputStream out);

}
