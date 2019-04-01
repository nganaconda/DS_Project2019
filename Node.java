import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;

public interface Node {

    public final static List<Broker> brokers = null;



    public void init(int i);

    public void connect();

    public void disconect(Socket requestSocket, ObjectInputStream in, ObjectOutputStream out);

    public void updateNodes();

}
