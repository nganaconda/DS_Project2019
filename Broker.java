import java.util.List;

public interface Broker extends Node {

    public final static List<Subscriber> registeredSubscribers = null;

    public final static List<Publisher> registeredPublishers = null;

    public void calculateKeys();

    public Publisher acceptConnection(Publisher p);

    public Subscriber acceptConnection(Subscriber p);

    public  void notifyPublisher(String s);

    public void pull(Topic t);


}
