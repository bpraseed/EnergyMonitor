import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by praseed on 11/12/15.
 */
public class HBaseConstants {
        static String tableName = "EnergyTable";
        static byte[] columnFamily = Bytes.toBytes("f");
        static byte[] column = Bytes.toBytes("a");
}