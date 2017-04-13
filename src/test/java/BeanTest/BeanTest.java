package BeanTest;

import StarModelBuilder.Location.City;
import StarModelBuilder.Location.Country;
import StarModelBuilder.Location.Province;
import org.apache.hadoop.fs.shell.Count;
import org.junit.Test;

/**
 * Created by Sha0w on 2017/4/13.
 */
public class BeanTest {
    @Test
    public void CityGen() {
        City city = new City("�人��");
        System.out.println(city.getId());
    }
    @Test
    public void ProvinceGen() {
        Province city = new Province("����ʡ");
        System.out.println(city.getId());
    }
    @Test
    public void CountryGen() {
        Country city = new Country("�й�");
        System.out.println(city.getId());
    }
}
