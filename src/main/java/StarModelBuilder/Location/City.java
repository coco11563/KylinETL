package StarModelBuilder.Location;

import static HbaseImporter.HbaseCeller.inial;

/**
 * Created by Sha0w on 2017/4/13.
 */
public class City {
    private String id;
    private String name;
    public City(String name) {
        setName(name);
        setId(inial.getCity_id(name));
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
