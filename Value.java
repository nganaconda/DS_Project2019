public class Value {

    private Bus bus;
    private double latitude, longtitude;

    public Value(Bus bus, double latitude, double longtitude) {
        this.bus = bus;
        this.latitude = latitude;
        this. longtitude = longtitude;
    }


    public Bus getBus() {
        return bus;
    }

    public void setBus(Bus bus) {
        this.bus = bus;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongtitude() {
        return longtitude;
    }

    public void setLongtitude(double longtitude) {
        this.longtitude = longtitude;
    }
}
