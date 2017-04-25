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
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
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
    private HTable _cityTable = null;
    private HTable _provinceTable  = null;
    private HTable _countryTable = null;
    private HTable _checkinTable = null;
    private HTable _timeTable = null;
    private HTable _userTable = null;
    private String filestatu;
    private int iter;
    ImportThread(String filestatu) {
        this.filestatu = filestatu;
        _cityTable = inialCityTable();
        _provinceTable = inialProvinceTable();
        _countryTable = inialCountryTable();
        _checkinTable = inialWeiboTable();
        _timeTable = inialTimeTable();
        _userTable = inialUserTable();
    }
    //����TL�洢�����Htable
    private final static ThreadLocal<HTable> cityTableLocal = new ThreadLocal<>();
    private final static ThreadLocal<HTable> provinceTableLocal = new ThreadLocal<>();
    private final static ThreadLocal<HTable> countryTableLocal = new ThreadLocal<>();
    private final static ThreadLocal<HTable> checkinTableLocal = new ThreadLocal<>();
    private final static ThreadLocal<HTable> timeTableLocal = new ThreadLocal<>();
    private final static ThreadLocal<HTable> userTableLocal = new ThreadLocal<>();
//    private final ArrayList<Put> putCityList = new ArrayList<>();
//    private final ArrayList<Put> putProvinceList = new ArrayList<>();
//    private final ArrayList<Put> putCountryList = new ArrayList<>();
//    private final ArrayList<Put> putTimeList = new ArrayList<>();
//    private final ArrayList<Put> putUserList = new ArrayList<>();
    private final ArrayList<Put> putCheckInList = new ArrayList<>();
    @Override
    public void run() {
//        String times = Integer.toString(iter);
//        long end_oneday_time = new Date().getTime();
//        long use_time = (end_oneday_time - start_oneday_time)/(1000*60);//���Ӵ洢����ʱ��
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
     * @param d ����
     * @param m ���ӵ�����
     * @return �����������������
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
            logger.error("smb��������");
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
            for (int rownum = 0; rownum < inputjson.size(); rownum++)//����������
            {
                JSONObject jcell = inputjson.getJSONObject(rownum);
                HbaseCeller hbaseCeller = new HbaseCeller(jcell);
                rowKey rowKey = hbaseCeller.getRowKey();
                OtherInform otherInform = hbaseCeller.getOtherInform();
                //��װCeller
                time = new Time(rowKey.dateFormat.toDate());
                city = new City(rowKey.city_name, rowKey.city_id);
                province = new Province(rowKey.province_name, rowKey.province_id);
                country = new Country(rowKey.country_name, rowKey.country_id);
                user = new User(rowKey.user_id, otherInform.getGender(), otherInform.getUsername());
                checkIn = new CheckIn(rowKey.weibo_id, rowKey.geo_Hash,
                        otherInform.getContent(), jcell.toString(),
                        city, province, country, time, user, rowKey.dateFormat.toDate(), otherInform.getPicURL());
                //������д��
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
                    _checkinTable.flushCommits();
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
                    logger.debug("����һ��д��");
                }
            }
//            _cityTable.put(putCityList);
            _checkinTable.put(putCheckInList);
//            _countryTable.put(putCountryList);
//            _provinceTable.put(putProvinceList);
//            _timeTable.put(putTimeList);
//            _userTable.put(putUserList);
//            _cityTable.flushCommits();
            _checkinTable.flushCommits();
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
            logger.debug("��β������һ��д��");
//            _cityTable.close();
//            _provinceTable.close();
//            _checkinTable.close();
//            _countryTable.close();
//            _timeTable.close();
//            _userTable.close();
            long end_oneday_time = new Date().getTime();
            logger.info("�ó���ʹ����" + (end_oneday_time - start_oneday_time) / 1000 + "��");
        } catch (ParseException | KeySizeException | IOException e1) {
            e1.printStackTrace();
        }
    }

    private static HTable inialCityTable() {
        HTable cityTable = cityTableLocal.get();
        if (cityTable == null) {
            try {
                cityTable = new HTable(cfg, ImportThread.cityTable);
                cityTable.setAutoFlushTo(false);
                cityTableLocal.set(cityTable);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return cityTable;
    }
    private static HTable inialProvinceTable() {
        HTable provincetable = provinceTableLocal.get();
        if (provincetable == null) {
            try {
                provincetable = new HTable(cfg, ImportThread.provinceTable);
                provincetable.setAutoFlushTo(false);
                provinceTableLocal.set(provincetable);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return provincetable;
    }
    private static HTable inialCountryTable() {
        HTable countryTable = countryTableLocal.get();
        if (countryTable == null) {
            try {
                countryTable = new HTable(cfg, ImportThread.countryTable);
                countryTable.setAutoFlushTo(false);
                countryTableLocal.set(countryTable);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return countryTable;
    }
    private static HTable inialTimeTable() {
        HTable timeTable = timeTableLocal.get();
        if (timeTable == null) {
            try {
                timeTable = new HTable(cfg, ImportThread.timeTable);
                timeTable.setAutoFlushTo(false);
                timeTableLocal.set(timeTable);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return timeTable;
    }
    private static HTable inialUserTable() {
        HTable userTable = userTableLocal.get();
        if (userTable == null) {
            try {
                userTable = new HTable(cfg, ImportThread.userTable);
                userTable.setAutoFlushTo(false);
                userTableLocal.set(userTable);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return userTable;
    }

    private static HTable inialWeiboTable() {
        HTable weiboTable = checkinTableLocal.get();
        if (weiboTable == null) {
            try {
                weiboTable = new HTable(cfg, ImportThread.checkinTable);
                weiboTable.setAutoFlushTo(false);
                checkinTableLocal.set(weiboTable);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return weiboTable;
    }
}
