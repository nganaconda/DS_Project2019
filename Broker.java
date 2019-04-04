package DS_as1;

import java.util.ArrayList;
import java.util.List;

public interface Broker extends Node
{
    //public List<Subscriber> registeredSubscribers;

    public void calculateKeys();

    public void acceptConnection(PublisherImpl pub);

    public void acceptConnection(SubscriberImpl sub);

    public void notifyPublisher(Object msg);

    public Topic getInfo();

    public void pull(Topic topic);

    public int getPort();

    public String getIp();

    public int getHashipport();

    public void addTopics(Topic t);

    public ArrayList<Topic> getTopics();

    public void setTopics(ArrayList<Topic> t);
}
