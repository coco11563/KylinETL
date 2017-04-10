import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * Created by Administrator on 2017/4/10.
 */
public class HbaseTest {
    public static void main(String args[]) throws IOException {
//        System.setProperty("")
        Configuration cfg = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(cfg);
        TableName[] tableNames = admin.listTableNames();
        for (TableName tableName : tableNames) {
            System.out.println(tableName.getNameAsString());
//            admin.disableTable(tableName);
//            admin.deleteTable(tableName);
        }
        admin.close();
    }
}
