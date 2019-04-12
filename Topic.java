package DS_as1;

import java.io.Serializable;

public class Topic implements Serializable {


    private String lineCode, busLineId, desEng;

    public Topic(String lineCode, String busLineId, String desEng) {
        this.lineCode = lineCode;
        this.busLineId = busLineId;
        this.desEng = desEng;
    }

    public Topic(String busLineId) {
        this.busLineId = busLineId;
    }

    public String getBusLineId() {
        return busLineId;
    }

    public void setBusLineId(String busLineId) {
        this.busLineId = busLineId;
    }

    public String getLineCode() {
        return lineCode;
    }

    public void setLineCode(String lineCode) {
        this.lineCode = lineCode;
    }

    public String getDesEng() {
        return desEng;
    }

    public void setDesEng(String desEng) {
        this.desEng = desEng;
    }
}