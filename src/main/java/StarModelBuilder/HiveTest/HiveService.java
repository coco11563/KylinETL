package StarModelBuilder.HiveTest;



import java.sql.*;

/**
 * Created by Sha0w on 2017/4/11.
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.log4j.Logger;

public class HiveService {
    static Logger logger = Logger.getLogger(HiveService.class);
    //hive的jdbc驱动类
    public static String dirverName = "org.apache.hive.jdbc.HiveDriver";
    //连接hive的URL hive1.2.1版本需要的是jdbc:hive2，而不是 jdbc:hive
    public static String url = "jdbc:hive2://slave1.hadoop:10000/;transportMode=binary";
    /**
     * 创建连接
     * @return
     * @throws SQLException
     */
    public static Connection getConn(){
        Connection conn = null;
        try {
            Class.forName(dirverName);
            conn = DriverManager.getConnection(url,"ambari-qa","");
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 创建命令
     * @param conn
     * @return
     * @throws SQLException
     */
    public static Statement getStmt(Connection conn) throws SQLException{
        logger.debug(conn);
        if(conn == null){
            logger.debug("this conn is null");
        }
        return conn.createStatement();
    }

    /**
     * 关闭连接
     * @param conn
     */
    public static void closeConn(Connection conn){
        try {
            conn.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * 关闭命令
     * @param stmt
     */
    public static void closeStmt(Statement stmt){
        try {
            stmt.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
