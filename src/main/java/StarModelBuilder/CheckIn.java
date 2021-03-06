package StarModelBuilder;

import HbaseImporter.DatePart.dateUtil;
import StarModelBuilder.Location.Location;
import StarModelBuilder.Time.Time;
import StarModelBuilder.User.User;

import java.util.Date;

/**
 * Created by Sha0w on 2017/4/15.
 */
public class CheckIn {
    private double lat;
    private double lon;
    private String weibo_id;
    private String geoHash;
    private String content;
    private String poiid;
//    private String json_file;
    private String uid;
//    private String cid;
//    private String coid;
    private String tid;
//    private String pid;
    private String loid;
    private String unix_time;
    private String pic_Url;
    public CheckIn(String weibo_id, String geoHash, String content, Location location, Time time, User user, Date date, String pic_Url, double lat, double lon, String poiid) {
        setWeibo_id(weibo_id);
        setGeoHash(geoHash);
        setContent(content);
//        setJson_file(json_file);
//        setCid(city.getId());
//        setPid(province.getId());
//        setCoid(country.getId());
        setLoid(location.getLoid());
        setTid(time.getTime_id());
        setUid(user.getUser_id());
        setUnix_time(dateUtil.format(date));
        setLat(lat);
        setLon(lon);
        if (pic_Url == null) {
            setPic_Url("0");
        } else {
            setPic_Url(pic_Url);
        }
        if (poiid == null) {
            setPoiid("NE");
        } else {
            setPoiid(poiid);
        }
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

//    public String getJson_file() {
//        return json_file;
//    }
//
//    public void setJson_file(String json_file) {
//        this.json_file = json_file;
//    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

//    public String getCid() {
//        return cid;
//    }

//    public void setCid(String cid) {
//        this.cid = cid;
//    }

//    public String getCoid() {
//        return coid;
//    }

//    public void setCoid(String coid) {
//        this.coid = coid;}

    public String getTid() {
        return tid;
    }

    public void setTid(String tid) {
        this.tid = tid;
    }

//    public String getPid() {
//        return pid;
//    }
//
//    public void setPid(String pid) {
//        this.pid = pid;
//    }
    public String getUnix_time() {
        return unix_time;
    }

    public void setUnix_time(String unix_time) {
        this.unix_time = unix_time;
    }

    public String getPic_Url() {
        return pic_Url;
    }

    public void setPic_Url(String pic_Url) {
        this.pic_Url = pic_Url;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public String getLoid() {
        return loid;
    }

    public void setLoid(String loid) {
        this.loid = loid;
    }

    public String getPoiid() {
        return poiid;
    }

    public void setPoiid(String poiid) {
        this.poiid = poiid;
    }
}
