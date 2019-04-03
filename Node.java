package DS_as1;

import java.util.ArrayList;
import java.util.List;

public interface Node {
    List<BrokerImpl> brokers = new ArrayList<BrokerImpl>() {
        {
            for(int i = 0; i < 4; i++) {
                add(new BrokerImpl(Integer.toString(i)));
            }
        }
    };

    public void init(int x);

    public void connect();

    //public void disconnect();

    //public void updateNodes();
}
