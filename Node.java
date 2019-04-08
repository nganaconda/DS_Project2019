import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public interface Node {

    List<Broker> brokers = new ArrayList<Broker>() {
        {
            add(new BrokerImpl1("192.168.1.7", 1000));
            add(new BrokerImpl1("192.168.1.7", 2000));
        }
    };



    public void init(); // int i

    public void connect();

    public void disconnect(Socket requestSocket);

    //public void updateNodes();

}
