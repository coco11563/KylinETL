package HbaseImporter;

import StarModelBuilder.CheckIn;
import StarModelBuilder.Location.City;
import StarModelBuilder.Location.Country;
import StarModelBuilder.Location.Province;
import StarModelBuilder.Time.Time;
import StarModelBuilder.User.User;
import datastruct.KeySizeException;
import jcifs.smb.SmbFile;
import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import static HbaseImporter.HbaseImporter.cfg;
import static HbaseImporter.HbaseImporter.completeddate;
import static HbaseImporter.HbaseImporter.completednum;
import static HbaseUtil.HbaseOperation.*;
import static HbaseUtil.HbaseOperation.putCheckIn;
import static HbaseUtil.HbaseOperation.putUser;

/**
 * Created by Administrator on 2017/4/19.
 *
 */
public class ImportThread implements Runnable {
    private static String cityTable = "weibodata.city_table";
    private static String provinceTable = "weibodata.province_table";
    private static String countryTable = "weibodata.country_table";
    private static String checkinTable = "weibodata.check_in_table";
    private static String timeTable = "weibodata.time_table";
    private static String userTable = "weibodata.user_table";
    private static Logger logger = Logger.getLogger(ImportThread.class);
    private Table _cityTable = null;
    private Table _provinceTable  = null;
    private Table _countryTable = null;
    private Table _checkinTable = null;
    private Table _timeTable = null;
    private Table _userTable = null;
    private String filestatu;
    private int iter;
    ImportThread(String filestatu) {
        this.filestatu = filestatu;
        _cityTable = getTable(cityTable);
        _provinceTable = getTable(provinceTable);
        _countryTable = getTable(countryTable);
        _checkinTable = getTable(checkinTable);
        _timeTable = getTable(timeTable);
        _userTable = getTable(userTable);
    }
    private final static ThreadLocal<Connection> connectionLocal = new ThreadLocal<>();
    private final ArrayList<Put> putCityList = new ArrayList<>();
    private final ArrayList<Put> putProvinceList = new ArrayList<>();
    private final ArrayList<Put> putCountryList = new ArrayList<>();
    private final ArrayList<Put> putTimeList = new ArrayList<>();
    private final ArrayList<Put> putUserList = new ArrayList<>();
    private final ArrayList<Put> putCheckInList = new ArrayList<>();

    @Override
    public void run() {
//        String times = Integer.toString(iter);
//        long end_oneday_time = new Date().getTime();
//        long use_time = (end_oneday_time - start_oneday_time)/(1000*60);//分钟存储运行时间
//        try {
//            numInsert(dateplus(start,iter),stornum) ;
//            dateInsert((times+"_"+use_time), dateplus(start,iter));
//        } catch (Exception e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
        Import(filestatu);
    }
    @SuppressWarnings("unused")
    private static void dateInsert(String iter,String date) throws JSONException, IOException {
        // TODO Auto-generated method stub
        JSONObject cd = new JSONObject();
        cd.element("completedate_" + iter,date );
        File f = new File(completeddate);
        if(!f.exists()){
            f.createNewFile();
        }
        FileWriter fileWritter = new FileWriter(f.getName(),true);
        BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
        bufferWritter.write(cd.toString());
        bufferWritter.close();

        logger.info("Done One day");
    }
    private static void numInsert(String date,int stornum) throws JSONException, IOException {
        // TODO Auto-generated method stub
        JSONObject cd = new JSONObject();
        cd.element(date,stornum );
        File f = new File(completednum);
        if(!f.exists()){
            f.createNewFile();
        }
        FileWriter fileWritter = new FileWriter(f.getName(),true);
        BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
        bufferWritter.write(cd.toString());
        bufferWritter.close();

        logger.info("log one day num");
    }

    /**
     *
     * @param d 日期
     * @param m 增加的天数
     * @return 增加完天数后的日期
     */
    static String dateplus(Date d, int m)
    {
        Calendar cal = Calendar.getInstance();
        cal.setTime(d);
        cal.add(Calendar.DATE, m);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        return df.format((cal.getTime()));
    }
    private void Import(String filestatu) {
        SmbFile remotefs = null;
        try {
            remotefs = new SmbFile(filestatu);
            remotefs.setConnectTimeout(600000);
        } catch (MalformedURLException e) {
            logger.error("smb出现问题");
            e.printStackTrace();
        }
        long start_oneday_time = new Date().getTime();
        JSONArray inputjson = null;
        try {
            inputjson = Read.read_jsonFile(remotefs, "utf-8");
//            stornum .addAndGet( inputjson.size() );

            Time time;
            City city;
            Country country;
            Province province;
            User user;
            CheckIn checkIn;
//            Put ptime;
//            Put pcity;
//            Put pprovince;
//            Put pcountry;
//            Put puser;
            Put pcheckin;
            for (int rownum = 0; rownum < inputjson.size(); rownum++)//按行数遍历
            {
                JSONObject jcell = inputjson.getJSONObject(rownum);
                HbaseCeller hbaseCeller = new HbaseCeller(jcell);
                rowKey rowKey = hbaseCeller.getRowKey();
                OtherInform otherInform = hbaseCeller.getOtherInform();
                //分装Celler
                time = new Time(rowKey.dateFormat.toDate());
                city = new City(rowKey.city_name, rowKey.city_id);
                province = new Province(rowKey.province_name, rowKey.province_id);
                country = new Country(rowKey.country_name, rowKey.country_id);
                user = new User(rowKey.user_id, otherInform.getGender(), otherInform.getUsername());
                checkIn = new CheckIn(rowKey.weibo_id, rowKey.geo_Hash,
                        otherInform.getContent(), jcell.toString(),
                        city, province, country, time, user, rowKey.dateFormat.toDate(), otherInform.getPicURL(),rowKey.lat,rowKey.lon);
                //插入流写入
//                ptime = putTime(time);
//                pcity = putCity(city);
//                pprovince = putProvince(province);
//                pcountry = putCountry(country);
//                puser = putUser(user);
                pcheckin = putCheckIn(checkIn);
//                putTimeList.add(ptime);
                putCheckInList.add(pcheckin);
//                putUserList.add(puser);
//                putCountryList.add(pcountry);
//                putProvinceList.add(pprovince);
//                putCityList.add(pcity);
                if (putCheckInList.size() > 1000) {
//                    _cityTable.put(putCityList);
                    _checkinTable.put(putCheckInList);
//                    _countryTable.put(putCountryList);
//                    _provinceTable.put(putProvinceList);
//                    _timeTable.put(putTimeList);
//                    _userTable.put(putUserList);
//                    _cityTable.flushCommits();


//                    _checkinTable.flushCommits();
//                    _countryTable.flushCommits();
//                    _provinceTable.flushCommits();
//                    _timeTable.flushCommits();
//                    _userTable.flushCommits();


                    putCheckInList.clear();
//                    putCityList.clear();
//                    putCountryList.clear();
//                    putProvinceList.clear();
//                    putTimeList.clear();
//                    putUserList.clear();
                    logger.debug("进行一次写入");
                }
            }
//            _cityTable.put(putCityList);
            _checkinTable.put(putCheckInList);
//            _countryTable.put(putCountryList);
//            _provinceTable.put(putProvinceList);
//            _timeTable.put(putTimeList);
//            _userTable.put(putUserList);
//            _cityTable.flushCommits();


//            _checkinTable.flushCommits();
//            _countryTable.flushCommits();
//            _provinceTable.flushCommits();
//            _timeTable.flushCommits();
//            _userTable.flushCommits();


            putCheckInList.clear();
//            putCityList.clear();
//            putCountryList.clear();
//            putProvinceList.clear();
//            putTimeList.clear();
//            putUserList.clear();
            logger.debug("结尾处进行一次写入");
//            _cityTable.close();
//            _provinceTable.close();
//            _checkinTable.close();
//            _countryTable.close();
//            _timeTable.close();
//            _userTable.close();
            long end_oneday_time = new Date().getTime();
            logger.info("该城市使用了" + (end_oneday_time - start_oneday_time) / 1000 + "秒");
        } catch (ParseException | KeySizeException | IOException e1) {
            e1.printStackTrace();
        }
    }
    private static Connection getConnection() {
        Connection connection = connectionLocal.get();
        if (connection == null) {
            try {
                connection = ConnectionFactory.createConnection(cfg);
                connectionLocal.set(connection);
                return connection;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }
    private static Table getTable(String tN) {
        Connection connection = getConnection();
        TableName tableName = TableName.valueOf(tN);
        try {
            return connection.getTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
