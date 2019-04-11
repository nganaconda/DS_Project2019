package DS_as1;

import java.util.ArrayList;
import java.util.List;

public interface Node {
    ArrayList<Broker> brokers = new ArrayList<Broker>() {
        {
            add(new BrokerImpl1(0, "192.168.1.8", 1000));
            add(new BrokerImpl1(1, "192.168.1.8", 2000));
        }
    };

    public void init(int x);

    public void connect();

    //public void disconnect();

    //public void updateNodes();
}
