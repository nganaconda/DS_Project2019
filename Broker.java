import java.util.ArrayList;
import java.util.List;

public interface Broker extends Node {

    public final static List<Subscriber> registeredSubscribers = null;

    public final static List<Publisher> registeredPublishers = null;

    public void calculateKeys();

    public void acceptConnection(Publisher p);

    public void acceptConnection(SubscriberImpl p);

    public  void notifyPublisher(Object msg);

    public Topic getInfo();

    public void pull(Topic t);

    public int getPort();

    public String getIp();

    public int getHashipport();

    public void addTopics(Topic t);

    public ArrayList<Topic> getTopics();

    public void setTopics(ArrayList<Topic> t);

    public int getBrokerId();

}
