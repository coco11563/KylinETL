package BeanTest;

import StarModelBuilder.Location.City;
import StarModelBuilder.Location.Country;
import StarModelBuilder.Location.Province;
import StarModelBuilder.Time.Time;
import org.apache.hadoop.fs.shell.Count;
import org.junit.Test;

import java.util.Date;

/**
 * Created by Sha0w on 2017/4/13.
 */
public class BeanTest {
    @Test
    public void CityGen() {
        City city = new City("武汉市");
        System.out.println(city.getId());
    }
    @Test
    public void ProvinceGen() {
        Province city = new Province("湖北省");
        System.out.println(city.getId());
    }
    @Test
    public void CountryGen() {
        Country city = new Country("中国");
        System.out.println(city.getId());
    }
    @Test
    public void TimeGen() {
        Time time = new Time(new Date());
        System.out.println(time.getDate().toString() + "," + time.getTime_id() + "," + time.getDay());
    }

}
