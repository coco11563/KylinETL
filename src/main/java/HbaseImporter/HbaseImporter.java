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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;


import static HbaseImporter.ImportThread.dateplus;
import static HbaseImporter.ZipPart.GetFileStatus.showAllFiles;
import static HbaseUtil.HbaseOperation.*;

public class HbaseImporter {
    private final static String cityNumPath = "./src/main/conf/cityNum.json";
    //2016-02-24 -->  2016-10-05
    private final static String timesetpath = "./src/main/conf/timeSetting.json";
    public final static String completeddate = "./src/main/conf/CompletedDate.json";
    public final static String completednum = "./src/main/conf/completednum.json";

    // ��ȡHBaseConfiguration
    public static Configuration cfg = HBaseConfiguration.create();


    private static String TableName = "SinaWeiboDataStorage";
    //log4j initial
    //counter
    public static AtomicInteger atomicInteger = new AtomicInteger(0);


    private static Logger logger = Logger.getLogger(HbaseImporter.class);


    @SuppressWarnings("deprecation")
    public static void main(String[] agrs) throws JSONException, ParseException, IOException {
        ExecutorService es = Executors.newFixedThreadPool(9);
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
        for(int iter = 0 ; iter < days ; iter ++)//����
        {
            AtomicInteger stornum = new AtomicInteger(0);
            String smbstring = "smb://biggrab:123456@192.168.1.111/biggrab/export/"+dateplus(start,iter)+"/";
            String smbzipstring = "smb://biggrab:123456@192.168.1.111/biggrab/export/"+dateplus(start,iter)+".zip";
            SmbFile fs = new SmbFile(smbstring);
//            System.out.println(smbstring);
            if (!fs.exists()) {
                continue;
            }
            List<String> filestatus = showAllFiles(fs);
            for (String filestatu : filestatus) {//�����б���

//                if (filestatu.split("/").length == 8) {
//                    logger.debug("������:" + ((filestatu.split("/"))[7]).split("\\.")[0]);
//                    logger.debug("����:" + TableName);
//                } else {
//                    logger.debug("������:" + ((filestatu.split("/"))[6]).split("\\.")[0]);
//                    logger.debug("����:" + TableName);
//                }
                try {
                    es.submit(new ImportThread(filestatu));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
//            //��ʼѹ����ҵ
//            logger.info("start to zip the file");
//            createSmbZip(smbstring,smbzipstring);
//            logger.info("zip over");
//            fs.delete();
        }
        while (es.isTerminated()){
            es.shutdown();
        }
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
