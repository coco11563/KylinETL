package StarModelBuilder.Location;

/**
 * Created by Administrator on 2017/5/12.
 */
public class Location {
    private String loid, coid, pid, cid;
    public Location(String coid, String pid, String cid) {
        setPid(pid);
        setCoid(coid);
        setCid(cid);
        setLoid(coid + pid + cid);
    }
    public Location(String coid, String cid) {
        String pid = cid.substring(0, 2);
        setPid(pid);
        setCoid(coid);
        setCid(cid.substring(2,4));
        setLoid(coid + cid);
    }
    public Location(City city, Country country) {
        String _cid = city.getId();
        String pid = _cid.substring(0, 2);
        setPid(pid);
        setCoid(country.getId());
        setCid(_cid.substring(2,4));
        setLoid(country.getId()+city.getId());
    }
    public String getCid() {
        return cid;
    }

    public void setCid(String cid) {
        this.cid = cid;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getCoid() {
        return coid;
    }

    public void setCoid(String coid) {
        this.coid = coid;
    }

    public String getLoid() {
        return loid;
    }

    public void setLoid(String loid) {
        this.loid = loid;
    }

    public static void main(String args[]) {
        Location location = new Location("00", "1233");
        System.out.println(location.getCid());
        System.out.println(location.getPid());
    }
}
