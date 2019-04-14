package DS_as1;

public interface Publisher extends Node
{
    //public void getBrokerList();

    //public Broker hashTopic(Topic topic);

    public void push(String busLineId);

    public void notifyFailure();
}
