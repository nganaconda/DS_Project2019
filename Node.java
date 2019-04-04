package DS_as1;

import java.util.ArrayList;
import java.util.List;

public interface Node {
    List<Broker> brokers = new ArrayList<Broker>() {
        {
            add(new BrokerImpl1("192.168.1.7", 1000));
            add(new BrokerImpl1("192.168.1.7", 2000));
        }
    };

    public void init(int x);

    public void connect();

    //public void disconnect();

    //public void updateNodes();
}
