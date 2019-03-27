package DS_as1;

import java.util.List;

public interface Broker extends Node
{
    //public List<Subscriber> registeredSubscribers;
    //public List<Publisher> registeredPublishers;

    public void calculateKeys();

    public void acceptConnection(Publisher pub);

    public void acceptConnection(Subscriber sub);

    public void notifyPublisher(String msg);

    //public void pull(Topic topic);
}
