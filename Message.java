public class Message {

    private static final long serialVersionUID = 3707231263354194674L;
    int id;
    String data;

    public Message(int id, String data) {
        super();
        this.id = id;
        this.data = data;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String toString() {
        return id + " - " + data;
    }


}
