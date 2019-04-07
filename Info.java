package DS_as1;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

public class Info implements Serializable
{
    public BrokerImpl1 listOfBrokers;

    public Info(BrokerImpl1 br){
        listOfBrokers = br;
    }
}
