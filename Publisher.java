public interface Publisher extends Node {

    public void getBrokerList();

    public void hashTopic(Topic t);

    public void push(Topic t, Value v);

    public void notifyFailure(Broker b);

}
