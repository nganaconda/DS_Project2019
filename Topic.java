package DS_as1;

import java.io.Serializable;

public class Topic implements Serializable
{
    private String busLine;

    public Topic(String busL)
    {
        busLine = busL;
    }

    public String getBusLine(){
        return busLine;
    }
}
