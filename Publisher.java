package DS_as1;

import java.util.ArrayList;

public interface Publisher extends Node
{
    public int getID();

    public int getPort();

    public String getIP();

    public ArrayList<Topic> getTopics();

    public void setTopics(ArrayList<Topic> topics);

    public ArrayList<Value> getValues();

    public void setValues(ArrayList<Value> values);

    public ArrayList<Bus> getBuses();

    public void setBuses(ArrayList<Bus> buses);

    public void push(String busLineId);

    public void notifyFailure();
}
