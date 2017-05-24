package HBaseModifier;

import StarModelBuilder.Time.Time;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.*;

import static HbaseImporter.ImportThread.getTable;
import static HbaseImporter.ImportThread.timeTable;
import static HbaseUtil.HbaseOperation.putTime;
import static HbaseUtil.HbaseOperation.scan;
import static Util.FileUtil.fileRead;

/**
 * Created by Administrator on 2017/5/24.
 */
public class TimeTableBuilder {
    private static String columnFamily = "time_id";
    private static String tablename = "weibodata.time_table";
    private static String inputpath = "./src/main/resources/SQL/Table";
    private static Table _timeTable = getTable(timeTable);
    public static List<Put> liti = new ArrayList<>();
    public static void main(String args[]) {
        TimeIDModifier timeIDModifier = new TimeIDModifier();
        try {
            List<String[]> lis = fileRead("time_table.data","\t");
            Result temp = null;
           for (String[] tem: lis){
                liti.add(putTime(new Time(tem[0],tem[1],tem[2],tem[3],tem[4],tem[5]
                        )));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            _timeTable.put(liti);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @org.jetbrains.annotations.NotNull
    public static String rowModifier(byte[] row) {
        String rowN = Bytes.toString(row);
        return rowN.substring(0,4) + "-" + rowN.substring(4,6) + "-" + rowN.substring(6,8);
    }
    @Test
    public void testModifier() {
        System.out.print(rowModifier("20120202".getBytes()));
    }

    public static List<String[]> fileRead(String FileName, String split) throws FileNotFoundException {
        File f = new File(inputpath + "/" + FileName);
        if(!f.exists()) {
            throw new FileNotFoundException();
        } else {
            LinkedList<String[]> temp = new LinkedList<>();
            Reader in = null;
            BufferedReader br = null;
            Charset cs = Charset.forName("UTF-8");

            try {
                String tempString = null;
                in = new FileReader(f);
                br = new BufferedReader(in);
                while((tempString = br.readLine()) != null) {
                    String[] bufferResult = tempString.split(split);
                    temp.add(bufferResult);
                }
            } catch (Exception var18) {
                var18.getStackTrace();
            } finally {
                try {
                    assert in != null;

                    in.close();
                } catch (IOException var17) {
                    var17.printStackTrace();
                }

            }

            return temp;
        }
    }
}
