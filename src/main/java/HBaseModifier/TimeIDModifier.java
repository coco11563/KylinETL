package HBaseModifier;

import StarModelBuilder.Time.Time;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static HbaseImporter.ImportThread.getTable;
import static HbaseImporter.ImportThread.timeTable;
import static HbaseUtil.HbaseOperation.putTime;
import static HbaseUtil.HbaseOperation.scan;

/**
 * Created by coco1 on 2017/5/23.
 */
public class TimeIDModifier {
    private static String columnFamily = "time_id";
    private static String tablename = "weibodata.time_table";
    private static Table _timeTable = getTable(timeTable);
    public static List<Put> liti = new ArrayList<>();
    public static void main(String args[]) {
        TimeIDModifier timeIDModifier = new TimeIDModifier();
        try {
            ResultScanner rs = scan(tablename);
            Iterator<Result> iR = rs.iterator();
            Result temp = null;
            while ((temp = iR.next()) != null) {
                liti.add(putTime(new Time(
                        Bytes.toString(temp.getValue(columnFamily.getBytes(),"date_time".getBytes())),
                        Bytes.toString(temp.getValue(columnFamily.getBytes(),"year".getBytes())),
                        Bytes.toString(temp.getValue(columnFamily.getBytes(),"month".getBytes())),
                        Bytes.toString(temp.getValue(columnFamily.getBytes(),"day".getBytes())),
                        Bytes.toString(temp.getValue(columnFamily.getBytes(),"is_holiday".getBytes())),
                        rowModifier(temp.getRow()))));
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
}
