package DS_as1;

public interface Subscriber extends Node {
    public void connect();

    public Tuple<Value> ask(String line);

    public void visualiseData(Tuple<Value> finalreply, String line);

    public void disconnect();
}
