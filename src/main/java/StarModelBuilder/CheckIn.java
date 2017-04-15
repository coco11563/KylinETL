package StarModelBuilder;

import StarModelBuilder.Location.City;
import StarModelBuilder.Location.Country;
import StarModelBuilder.Location.Province;
import StarModelBuilder.Time.Time;
import StarModelBuilder.User.User;

/**
 * Created by Sha0w on 2017/4/15.
 */
public class CheckIn {
    private String weibo_id;
    private String geoHash;
    private String content;
    private String json_file;
    private String uid;
    private String cid;
    private String coid;
    private String tid;
    private String pid;
    public CheckIn(String weibo_id, String geoHash, String content, String json_file, City city, Province province, Country country, Time time, User user) {
        setWeibo_id(weibo_id);
        setGeoHash(geoHash);
        setContent(content);
        setJson_file(json_file);
        setCid(city.getId());
        setPid(province.getId());
        setCoid(country.getId());
        setTid(time.getTime_id());
        setUid(user.getUser_id());
    }
    public String getWeibo_id() {
        return weibo_id;
    }

    public void setWeibo_id(String weibo_id) {
        this.weibo_id = weibo_id;
    }

    public String getGeoHash() {
        return geoHash;
    }

    public void setGeoHash(String geoHash) {
        this.geoHash = geoHash;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getJson_file() {
        return json_file;
    }

    public void setJson_file(String json_file) {
        this.json_file = json_file;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getCid() {
        return cid;
    }

    public void setCid(String cid) {
        this.cid = cid;
    }

    public String getCoid() {
        return coid;
    }

    public void setCoid(String coid) {
        this.coid = coid;
    }

    public String getTid() {
        return tid;
    }

    public void setTid(String tid) {
        this.tid = tid;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }
}
