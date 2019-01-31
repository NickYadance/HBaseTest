import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseTest {
    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    protected static final String TABLE = "testTable";
    protected static final String COLUMN_FAMILY = "cf";
    protected static final String QUALIFIER = "a";
    protected static final String VALUE = "value";
    protected static final int PAGE_SZE = 1000;
    protected static final int CACHING = 300;
    protected static final String[] splitPoints = new String[]{"000", "111", "222", "333", "444", "555" };

    protected static Configuration CONFIG;
    protected static HBaseTestingUtility utility;
    protected static MiniHBaseCluster hBaseCluster;
    protected static Connection connection;

    static {
        utility = new HBaseTestingUtility();
        try{
            hBaseCluster = utility.startMiniCluster(2);
            CONFIG  = hBaseCluster.getConf();
            connection = ConnectionFactory.createConnection(CONFIG);
        } catch (Exception e){
        }
    }
}
