package HbaseUtil;

import HbaseImporter.DatePart.dateUtil;
import HbaseImporter.HbaseCeller;
import StarModelBuilder.CheckIn;
import StarModelBuilder.Location.City;
import StarModelBuilder.Location.Country;
import StarModelBuilder.Location.Location;
import StarModelBuilder.Location.Province;
import StarModelBuilder.Time.Time;
import StarModelBuilder.User.User;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import static HbaseImporter.HbaseImporter.cfg;

/**
 * Created by coco1 on 2017/1/27.
 *
 */
public class HbaseOperation {

    public static final String columnFamily = "sinadata";


    private static Logger logger = Logger.getLogger(HbaseOperation.class);

    // HBaseAdmin HTableDescriptor
    @SuppressWarnings("deprecation")
    public static void create(String tableName)throws IOException {
        logger.info("正在创建"+tableName);
        HBaseAdmin admin = new HBaseAdmin(cfg);
        if (!admin.tableExists(tableName)) {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDesc);
        }
        admin.close();
    }

    public static void create(String tablename,String columnFamily) throws Exception {
        @SuppressWarnings({ "resource", "deprecation" })
        HBaseAdmin admin = new HBaseAdmin(cfg);
        System.out.print(cfg);
        if (admin.tableExists(Bytes.toBytes(tablename))) {
            logger.error("table Exists!");
        }
        else{
            logger.info("create table . . .");
            @SuppressWarnings("deprecation")
            HTableDescriptor tableDesc = new HTableDescriptor(tablename);
            tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDesc);
            if (admin.tableExists(Bytes.toBytes(tablename))) {
                logger.info("create table success!");
            }

        }
    }


    /**
     *
     * @param tablename 表名
     * @param row 列名
     * @param columnFamily cF
     * @param column 数据名
     * @param data 数据
     * @throws Exception
     */
    public static void put(String tablename, String row, String columnFamily, String column, String data)
            throws Exception {
        @SuppressWarnings({ "resource" })
        HTable table = new HTable(cfg, tablename);
        Put p1 = new Put(Bytes.toBytes(row));
        p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
        table.put(p1);
        logger.info("put '" + row + "','" + columnFamily + ":" + column + "','" + data + "'");
    }
    /**
     *
     * @param tablename
     * @param row
     * @param columnFamily
     * @param array mapper值对键
     * @return put
     * @throws Exception
     */
    public static Put put(String tablename, String row,String columnFamily, HashMap<String,String> array )
            throws Exception {
        Put p1 = new Put(Bytes.toBytes(row));
        Set<String> keyset = array.keySet();
        for(Iterator<String> it = keyset.iterator(); it.hasNext() ;)//使用了一个很棒的办法遍历了map
        {
            String key = it.next();
            if(array.get(key) != null)
            {
                p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(key), Bytes.toBytes(array.get(key)));
            }
            else
            {
                logger.info("好險，避免了一次空指針");
            }
        }
        return p1;
    }
    public static Put put(HbaseCeller hbaseCeller, String columnFamily){
        Put p1 = new Put(Bytes.toBytes(hbaseCeller.getRowKey().toString()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("otherInform"), Bytes.toBytes(hbaseCeller.getOtherInform().getOtherInform()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("gender"), Bytes.toBytes(hbaseCeller.getOtherInform().getGender()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("picURL"), Bytes.toBytes(hbaseCeller.getOtherInform().getPicURL()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("content"), Bytes.toBytes(hbaseCeller.getOtherInform().getContent()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("username"), Bytes.toBytes(hbaseCeller.getOtherInform().getUsername()));
        return p1;
    }

    public static void get(String tablename, String row) throws IOException {
        @SuppressWarnings({ "deprecation", "resource" })
        HTable table = new HTable(cfg, tablename);
        Get g = new Get(Bytes.toBytes(row));
        Result result = table.get(g);
        logger.info("Get: " + result);
    }


    public static void scan(String tablename) throws Exception {
        @SuppressWarnings({ "deprecation", "resource" })
        HTable table = new HTable(cfg, tablename);
        Scan s = new Scan();
        ResultScanner rs = table.getScanner(s);
        for (Result r : rs) {
            logger.info("Scan: " + r);
        }
    }

    public static boolean delete(String tablename) throws IOException {

        @SuppressWarnings({ "deprecation", "resource" })
        HBaseAdmin admin = new HBaseAdmin(cfg);
        if (admin.tableExists(tablename)) {
            try {
                admin.disableTable(tablename);
                admin.deleteTable(tablename);
            } catch (Exception ex) {
                ex.printStackTrace();
                return false;
            }

        }
        return true;
    }

    public static void main(String args[]) throws IOException {
//        System.setProperty("")
        HBaseAdmin admin = new HBaseAdmin(cfg);
        TableName[] tableNames = admin.listTableNames("^city_.*");
        for (TableName tableName : tableNames) {
            System.out.println(tableName.getNameAsString());
//            admin.disableTable(tableName);
//            admin.deleteTable(tableName);
        }
        admin.close();
    }
    public static Put putTime(Time time){
        String columnFamily = "time_id";
        Put p1 = new Put(Bytes.toBytes(time.getTime_id()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("year"), Bytes.toBytes(time.getYear()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("month"), Bytes.toBytes(time.getMonth()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("day"), Bytes.toBytes(time.getDay()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("is_holiday"), Bytes.toBytes(time.getIs_holiday()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("date_time"), Bytes.toBytes(time.getDate()));
        return p1;
    }
    public static String dateBuilder(Date date) {
        return dateUtil.formatFromNormal(date);
    }
    public static Put putCity(City city) {
        String columnFamily = "city_id";
        Put p1 = null;
        try {
            p1 = new Put(Bytes.toBytes(city.getId()));
            p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("city_name"), Bytes.toBytes(city.getName()));
        } catch (NullPointerException e) {
            e.printStackTrace();
        }

        return p1;
    }
    public static Put putProvince(Province province) {
        String columnFamily = "province_id";
        Put p1 = new Put(Bytes.toBytes(province.getId()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("province_name"), Bytes.toBytes(province.getName()));
        return p1;
    }
    public static Put putCountry(Country country) {
        String columnFamily = "country_id";
        Put p1 = new Put(Bytes.toBytes(country.getId()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("country_name"), Bytes.toBytes(country.getName()));
        return p1;
    }
    public static Put putUser(User user) {
        String columnFamily = "user_id";
        Put p1 = new Put(Bytes.toBytes(user.getUser_id()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("gender"), Bytes.toBytes(user.getGender()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("user_name"), Bytes.toBytes(user.getUsername()));
        return p1;
    }
    public static Put putCheckIn(CheckIn checkIn){
        String columnFamily = "weibo_id";
        Put p1 = new Put(Bytes.toBytes(checkIn.getWeibo_id()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("user_id"), Bytes.toBytes(checkIn.getUid()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("time_id"), Bytes.toBytes(checkIn.getTid()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("lo_id"), Bytes.toBytes(checkIn.getLoid()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("geohash"), Bytes.toBytes(checkIn.getGeoHash()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("content"), Bytes.toBytes(checkIn.getContent()));
//        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("json_file"), Bytes.toBytes(checkIn.getJson_file()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("unix_time"), Bytes.toBytes(checkIn.getUnix_time()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("pic_url"), Bytes.toBytes(checkIn.getPic_Url()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("lat"), Bytes.toBytes(checkIn.getLat()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("lon"), Bytes.toBytes(checkIn.getLon()));
        return p1;
    }
    public static Put putLocation(Location location) {
        String columnFamily = "lo_id";
        Put p1 = new Put(Bytes.toBytes(location.getLoid()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("co_id"), Bytes.toBytes(location.getCoid()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("p_id"), Bytes.toBytes(location.getPid()));
        p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("c_id"), Bytes.toBytes(location.getCid()));
        return p1;
    }
}
