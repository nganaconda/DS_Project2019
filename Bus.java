package DS_as1;

import java.io.Serializable;

public class Bus implements Serializable {

    private String routeCode,vehicleId, lineName, lineCode, info;


    public Bus(String lineCode, String routeCode, String vehicleId, String info) { // bus Positions.txt
        this.lineCode = lineCode;         // busLineId = lineId
        this.routeCode = routeCode;
        this.vehicleId = vehicleId;
        this.info = info;                   // info = timestampOfBusPosition
    }



    public String getRouteCode() {
        return routeCode;
    }

    public void setRouteCode(String routeCode) {
        this.routeCode = routeCode;
    }

    public String getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(String vehicleId) {
        this.vehicleId = vehicleId;
    }

    public String getLineName() {
        return lineName;
    }

    public void setLineName(String lineName) {
        this.lineName = lineName;
    }

    //public String getBuslineId() {
    //   return buslineId;
    //}

    //public void setBuslineId(String buslineId) {
    //     this.buslineId = buslineId;
    //}

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }


    public String getLineCode() {
        return lineCode;
    }

    public void setLineCode(String lineCode) {
        this.lineCode = lineCode;
    }
}