package HbaseImporter;

import HbaseImporter.ConfigurePart.Inial;
import HbaseImporter.DatePart.dateFormat;
import HbaseImporter.GeoHashPart.GeoHash;
import HbaseImporter.HolidayPart.ChineseHoliday;
import datastruct.KDTree;
import datastruct.KeySizeException;
import module.TestPoint;
import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

import static KDTreeProvider.getTree.getInnerConfigNormalKDTree;

/**
 * Created by coco1 on 2017/1/27.
 *
 */
public class rowKey {
    private static final Logger logger =Logger.getLogger(rowKey.class);
    private static KDTree<TestPoint> kdTree;
    static {
        try {
            kdTree = getInnerConfigNormalKDTree();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private String poiid;
    public String city_name;
    public String province_name;
    public String country_name;
    public String country_id;
    public String province_id;
    public String city_id;
    public String geo_Hash;
    public String user_id;
    public String weibo_id;
    public String year;
    public String month;
    public String day;
    public int isHoliday;
    public dateFormat dateFormat;
    public double lat;
    public double lon;
    public rowKey(JSONObject weiboInform, Inial inial) throws JSONException, ParseException, KeySizeException {
        JSONObject place = getPlace(weiboInform);
        if (place != null)
        {setPoiid(place.optString("poiid"));} else {
            setPoiid(null);
        }
         lat = weiboInform.getJSONObject("geo").getJSONArray("coordinates").getDouble(0);
         lon = weiboInform.getJSONObject("geo").getJSONArray("coordinates").getDouble(1);
        JSONArray url_object = weiboInform.optJSONArray("url_objects");
        if (url_object != null) {
            JSONObject object_1 = getObject_1(url_object);
            if (object_1 != null) {
                JSONObject object_2 = object_1.optJSONObject("object");
                if (object_2 != null) {
                    JSONObject address = object_2.optJSONObject("address");
                    if (address != null) {
                        this.country_name = address.getString("country");
                        this.city_name = address.getString("locality");
                        this.province_name = address.getString("region");
                        this.country_id = inial.getCountry_id(country_name);
                        this.city_id = inial.getCity_id(city_name);
                        this.province_id = inial.getProvince_id(province_name);
                    } else {
                        String[] cityInform = getCity(lat, lon) ;
                        this.country_id = "00";
                        this.country_name = "中国";
                        this.city_name = cityInform[1];
                        this.province_name = cityInform[0];
                        this.province_id = inial.getProvince_id(cityInform[0]);
                        this.city_id = inial.getCity_id(cityInform[1]);
//                        logger.error(country_id + "_" + province_id + "_" + city_id);
                    }
                } else {
                    String[] cityInform = getCity(lat, lon) ;
                    this.country_id = "00";
                    this.country_name = "中国";
                    this.city_name = cityInform[1];
                    this.province_name = cityInform[0];
                    this.province_id = inial.getProvince_id(cityInform[0]);
                    this.city_id = inial.getCity_id(cityInform[1]);
//                    logger.error(country_id + "_" + province_id + "_" + city_id);
                }
            } else {
                String[] cityInform = getCity(lat, lon) ;
                this.country_id = "00";
                this.country_name = "中国";
                this.city_name = cityInform[1];
                this.province_name = cityInform[0];
                this.province_id = inial.getProvince_id(cityInform[0]);
                this.city_id = inial.getCity_id(cityInform[1]);
//                logger.error(country_id + "_" + province_id + "_" + city_id);
            }
        } else {
            String[] cityInform = getCity(lat, lon) ;
            this.country_id = "00";
            this.country_name = "中国";
            this.city_name = cityInform[1];
            this.province_name = cityInform[0];
            this.province_id = inial.getProvince_id(cityInform[0]);
            this.city_id = inial.getCity_id(cityInform[1]);
//            logger.error(country_id + "_" + province_id + "_" + city_id);
        }
        if (this.city_id == null || this.province_id == null || this.country_id == null) {
//            logger.error("出现一个url_obj为空");
//            logger.error(weiboInform.toString());
            String[] cityInform = getCity(lat, lon) ;
            this.country_id = "00";
            this.country_name = "中国";
            this.city_name = cityInform[1];
            this.province_name = cityInform[0];
            this.province_id = inial.getProvince_id(cityInform[0]);
            this.city_id = inial.getCity_id(cityInform[1]);
        }
        this.geo_Hash = new GeoHash(lat, lon).getGeoHashBase32();
        this.user_id = weiboInform.getJSONObject("user").getString("id");
        this.weibo_id = weiboInform.getString("idstr");
        String[] date = weiboInform.getString("created_at").split(" ");
        this.dateFormat = new dateFormat(date);
        this.year = dateFormat.getYear();
        this.month = dateFormat.getMonth();
        this.day = dateFormat.getDay();
        this.isHoliday = ChineseHoliday.getHoliday(dateFormat.toDate());
    }


    /**
     *
     * @param lat
     * @param lng
     * @return []
     * @throws KeySizeException
     */
    private static String[] getCity(double lat, double lng) throws KeySizeException {
        TestPoint tp = kdTree.nearest(new double[]{lat, lng});
        String[] ret = new String[]{tp.getCity(), tp.getProvince()};
        return ret;
    }

    @Override
    public String toString() {
        return this.country_id + "_" + this.province_id + "_" + this.city_id + "_" +
                this.geo_Hash + "_" + this.year + "_" + this.month + "_" + this.day + "_" + this.isHoliday +
                "_" + this.user_id + "_" + this.weibo_id;
    }


    public JSONObject getObject_1(JSONArray jsonArray) {
        if (jsonArray.size() <= 0) {
            return null;
        }

        JSONObject ret = jsonArray.optJSONObject(0).optJSONObject("object");
        if (ret == null) {
            JSONObject jsonarray_1 = jsonArray.optJSONObject(1);
            if (jsonarray_1 == null) {
                return null;
            } else {
                return jsonarray_1.optJSONObject("object");
            }

        }
        return ret;
    }
    public JSONObject getPlace(JSONObject weiboInform) {
        JSONArray annotations = weiboInform.optJSONArray("annotations");
        if (annotations != null) {
            JSONObject _place = annotations.optJSONObject(0);
            if (_place != null) {
                return _place.optJSONObject("place");
            } else {
                return annotations.optJSONObject(1).optJSONObject("place");
            }
        } else return null;
    }
    public static void main(String args[]) throws KeySizeException {
        System.out.print(getCity(39.958972, 116.301934)[1]);
    }

    public String getPoiid() {
        return poiid;
    }

    public void setPoiid(String poiid) {
        this.poiid = poiid;
    }
}
