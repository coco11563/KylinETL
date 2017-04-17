package StarModelBuilder.HiveTableBuilder;

import StarModelBuilder.FileUtil;
import StarModelBuilder.HiveTest.HiveService;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.sql.Connection;

import static StarModelBuilder.HiveTest.HiveService.sqlExcute;

/**
 * Created by Administrator on 2017/4/12.
 */
public class TableBuilder {
    private static String checkin = "CHECK_IN_TABLE.sql";
    private static String user = "USER_TABLE.sql";
    private static String time = "TIME_TABLE.sql";
    private static String province = "PROVINCE_TABLE.sql";
    private static String city = "CITY_TABLE.sql";
    private static String country = "COUNTRY_TABLE.sql";

    public static boolean TableBuilder(String tableName) throws FileNotFoundException {
        String sql = FileUtil.fileRead(tableName);
        Connection hiveConnection = HiveService.getConn();
        return sqlExcute(sql, hiveConnection);
    }

    @Test
    public void tableBuilde() throws FileNotFoundException {
        TableBuilder(checkin);
        TableBuilder(user);
        TableBuilder(time);
        TableBuilder(province);
        TableBuilder(city);
        TableBuilder(country);
    }
}
