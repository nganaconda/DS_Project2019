package DS_as1;

public interface Subscriber extends Node
{
    //public void register(Broker brok, Topic topic);

    public void disconnect();

    public void visualiseData(Tuple<Value> finalreply, String line);
}
