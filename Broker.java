public interface Broker extends Node
{
    private List<Subscriber> registeredSubscribers;
    private List<Publisher> registeredPublishers;

    public void calculateKeys()
    {

    }

    public Publisher acceptConnection(Publisher)
    {

    }

    public Subscriber acceptConnection(Subscriber)
    {

    }

    public void notifyPublisher(String)
    {

    }

    public void pull(Topic)
    {

    }
}
