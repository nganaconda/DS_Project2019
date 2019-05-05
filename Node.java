package DS_as1;

import java.util.ArrayList;
import java.util.List;

public interface Node {
    ArrayList<Broker> brokers = new ArrayList<Broker>() {
        {
            add(new BrokerImpl(0, "192.168.1.5", 1000));
            add(new BrokerImpl(1, "192.168.1.5", 2000));
        }
    };

    ArrayList<Publisher> publishers = new ArrayList<Publisher>() {
        {
            add(new PublisherImpl("192.168.1.5",100 , 0));
            add(new PublisherImpl("192.168.1.5",200 , 1));
        }

    };

    public void init(int x);

    public void connect();

    //public void disconnect();

}
