import java.util.List;

public interface Node {

    public final static List<Broker> brokers = null;



    public void init(int i);

    public void connect();

    public void disconect();

    public void updateNodes();

}
