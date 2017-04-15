package StarModelBuilder.Location;

/**
 * Created by Sha0w on 2017/4/13.
 */
public class Country {
    private String name;
    private String id;
    public Country(String name) {
        setName(name);
        setId("00");
    }
    public Country(String name, String id) {
        setId(id);
        setName(name);
    }
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
