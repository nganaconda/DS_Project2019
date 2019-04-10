package DS_as1;

public class Bus
{
    private String lineNumber;
    private String routeCode;
    private String vehicleid;
    private String lineName;
    private String buslineid;
    private String info;

    public Bus(String lineN, String routeC, String vehicle, String lineNa, String busline, String inf){
        lineNumber = lineN;
        routeCode = routeC;
        vehicleid = vehicle;
        lineName = lineNa;
        buslineid = busline;
        info = inf;
    }


    public String getLineNumber() {
        return lineNumber;
    }

    public String getRouteCode() {
        return routeCode;
    }

    public String getVehicleid() {
        return vehicleid;
    }

    public String getLineName() {
        return lineName;
    }

    public String getBuslineid() {
        return buslineid;
    }

    public String getInfo() {
        return info;
    }

    public void setLineNumber(String lineNumber) {
        this.lineNumber = lineNumber;
    }

    public void setRouteCode(String routeCode) {
        this.routeCode = routeCode;
    }

    public void setVehicleid(String vehicleid) {
        this.vehicleid = vehicleid;
    }

    public void setLineName(String lineName) {
        this.lineName = lineName;
    }

    public void setBuslineid(String buslineid) {
        this.buslineid = buslineid;
    }

    public void setInfo(String info) {
        this.info = info;
    }

}
