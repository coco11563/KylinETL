package HbaseImporter;

import HbaseUtil.HbaseOperation;
import StarModelBuilder.CheckIn;
import StarModelBuilder.Location.City;
import StarModelBuilder.Location.Country;
import StarModelBuilder.Location.Province;
import StarModelBuilder.Time.Time;
import StarModelBuilder.User.User;
import jcifs.smb.SmbFile;


import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;


import static HbaseImporter.ZipPart.GetFileStatus.showAllFiles;
import static HbaseUtil.HbaseOperation.*;

public class HbaseImporter {
    private final static String cityNumPath = "./src/main/conf/cityNum.json";
    //2016-02-24 -->  2016-10-05
    private final static String timesetpath = "./src/main/conf/timeSetting.json";
    private final static String completeddate = "./src/main/conf/CompletedDate.json";
    private final static String completednum = "./src/main/conf/completednum.json";

    // ��ȡHBaseConfiguration
    public static Configuration cfg = HBaseConfiguration.create();


    private static String TableName = "SinaWeiboDataStorage";
    //log4j initial

    private static String cityTable = "weibodata.city_table";
    private static String provinceTable = "weibodata.province_table";
    private static String countryTable = "weibodata.country_table";
    private static String checkinTable = "weibodata.check_in_table";
    private static String timeTable = "weibodata.time_table";
    private static String userTable = "weibodata.user_table";

    private static Logger logger = Logger.getLogger(HbaseImporter.class);

    /**
     *
     * @param d ����
     * @param m ���ӵ�����
     * @return �����������������
     */
    public static String dateplus(Date d , int m)
    {
        Calendar cal = Calendar.getInstance();
        cal.setTime(d);
        cal.add(Calendar.DATE, m);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String returntype = df.format((cal.getTime()));
        return returntype;
    }
    @SuppressWarnings("deprecation")
    public static void main(String[] agrs) throws JSONException, ParseException, IOException {
        // ��ȡ���е�ID JSON�ļ�
        JSONObject cityNumObject = JSONObject.fromObject(Read.readJson(cityNumPath));
        JSONObject timesetting = JSONObject.fromObject(Read.readJson(timesetpath));
        //������ʼ��
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date start = df.parse(timesetting.getString("start-time"));
        Date end =  df.parse(timesetting.getString("end-time"));
        int days = (int)((end.getTime() - start.getTime())/86400000) + 1;
        logger.debug("��ʼ����" + days+ "�������д��. . . ");
        // ��ʼ����hbase��д
        JSONArray inputjson = new JSONArray();
        for(int iter = 0 ; iter < days ; iter ++)//����
        {
            int stornum = 0; //�洢д������
            long start_oneday_time = new Date().getTime();
            String smbstring = "smb://biggrab:123456@192.168.1.111/biggrab/export/"+dateplus(start,iter)+"/";
            String smbzipstring = "smb://biggrab:123456@192.168.1.111/biggrab/export/"+dateplus(start,iter)+".zip";
            SmbFile fs = new SmbFile(smbstring);
            System.out.println(smbstring);
            if (!fs.exists()) {
                continue;
            }
            List<String> filestatus = showAllFiles(fs);
            SmbFile remotefs;
            for (String filestatu : filestatus) {//�����б���

                if (filestatu.split("/").length == 8) {
                    logger.debug("������:" + ((filestatu.split("/"))[7]).split("\\.")[0]);
                    logger.debug("����:" + TableName);
                } else {
                    logger.debug("������:" + ((filestatu.split("/"))[6]).split("\\.")[0]);
                    logger.debug("����:" + TableName);
                }
                try {
                    remotefs = new SmbFile(filestatu);
                    remotefs.setConnectTimeout(600000);
                    inputjson = Read.read_jsonFile(remotefs, "utf-8");
                    stornum += inputjson.size();
                    HTable _cityTable = new HTable(cfg, cityTable);
                    HTable _provinceTable = new HTable(cfg, provinceTable);
                    HTable _countryTable = new HTable(cfg, countryTable);
                    HTable _checkinTable = new HTable(cfg, checkinTable);
                    HTable _timeTable = new HTable(cfg, timeTable);
                    HTable _userTable = new HTable(cfg, userTable);
                    ArrayList<Put> putCityList = new ArrayList<>();
                    ArrayList<Put> putProvinceList = new ArrayList<>();
                    ArrayList<Put> putCountryList = new ArrayList<>();
                    ArrayList<Put> putTimeList = new ArrayList<>();
                    ArrayList<Put> putUserList = new ArrayList<>();
                    ArrayList<Put> putCheckInList = new ArrayList<>();
                    Time time;
                    City city;
                    Country country;
                    Province province;
                    User user;
                    CheckIn checkIn;
                    Put ptime;
                    Put pcity;
                    Put pprovince;
                    Put pcountry;
                    Put puser;
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
                        ptime = putTime(time);
                        pcity = putCity(city);
                        pprovince = putProvince(province);
                        pcountry = putCountry(country);
                        puser = putUser(user);
                        pcheckin = putCheckIn(checkIn);
                        putTimeList.add(ptime);
                        putCheckInList.add(pcheckin);
                        putUserList.add(puser);
                        putCountryList.add(pcountry);
                        putProvinceList.add(pprovince);
                        putCityList.add(pcity);
                        if (putCheckInList.size() > 1000) {
                            _cityTable.put(putCityList);
                            _checkinTable.put(putCheckInList);
                            _countryTable.put(putCountryList);
                            _provinceTable.put(putProvinceList);
                            _timeTable.put(putTimeList);
                            _userTable.put(putUserList);
                            _cityTable.flushCommits();
                            _checkinTable.flushCommits();
                            _countryTable.flushCommits();
                            _provinceTable.flushCommits();
                            _timeTable.flushCommits();
                            _userTable.flushCommits();
                            putCheckInList.clear();
                            putCityList.clear();
                            putCountryList.clear();
                            putProvinceList.clear();
                            putTimeList.clear();
                            putUserList.clear();
                            logger.debug("����һ��д��");
                        }
                    }
                    _cityTable.put(putCityList);
                    _checkinTable.put(putCheckInList);
                    _countryTable.put(putCountryList);
                    _provinceTable.put(putProvinceList);
                    _timeTable.put(putTimeList);
                    _userTable.put(putUserList);
                    _cityTable.flushCommits();
                    _checkinTable.flushCommits();
                    _countryTable.flushCommits();
                    _provinceTable.flushCommits();
                    _timeTable.flushCommits();
                    _userTable.flushCommits();
                    putCheckInList.clear();
                    putCityList.clear();
                    putCountryList.clear();
                    putProvinceList.clear();
                    putTimeList.clear();
                    putUserList.clear();
                    logger.debug("��β������һ��д��");
                    _cityTable.close();
                    _provinceTable.close();
                    _checkinTable.close();
                    _countryTable.close();
                    _timeTable.close();
                    _userTable.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            //���һ���д��
            String times = Integer.toString(iter);
            long end_oneday_time = new Date().getTime();
            long use_time = (end_oneday_time - start_oneday_time)/(1000*60);//���Ӵ洢����ʱ��
            try {
                numInsert(dateplus(start,iter),stornum) ;
                dateInsert((times+"_"+use_time), dateplus(start,iter));
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
//            //��ʼѹ����ҵ
//            logger.info("start to zip the file");
//            createSmbZip(smbstring,smbzipstring);
//            logger.info("zip over");
//            fs.delete();
        }
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
    private static String getUserID(String JsonString) throws JSONException {
        // TODO Auto-generated method stub
        JSONObject j = JSONObject.fromObject(JsonString);
        String gender = j.getString("gender");
        String id = j.getString("id");
        String renturnstring = gender + "_" +  id;
        return renturnstring;
    }
}
