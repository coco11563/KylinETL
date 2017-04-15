package StarModelBuilder.Location;

import static HbaseImporter.HbaseCeller.inial;

/**
 * Created by Sha0w on 2017/4/13.
 */
public class Province {
    private String name;
    private String id;
    public Province(String name) {
        setName(name);
        setId(inial.getProvince_id(name));
    }
    public Province(String name, String id) {
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
