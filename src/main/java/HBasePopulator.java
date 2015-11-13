import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by praseed on 11/12/15.
 */
public class HBasePopulator {

    public static void main(String[] args) {
        try {
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", "injest-cluster-1.vpc.cloudera.com");  // Here we are running zookeeper locally

            Connection connection = ConnectionFactory.createConnection(config);
            populate(100,
                    10000,
                    1,
                    connection,
                    HBaseConstants.tableName);
            //megaScan(connection, HBaseConstants.tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    static private void populate(int numOfUsers,
                          int numOfRecords,
                          int waitTimeEvery1000,
                          Connection connection, String tableStr) {
        try {
            BufferedMutator bufferedMutator = connection.getBufferedMutator(TableName.valueOf(tableStr));
            EnegryMonitorDataGen enegryMonitorDataGen = new EnegryMonitorDataGen(numOfUsers);
            for (int i = 0; i < numOfRecords; i++) {
                EnegryMonitorRecord record = enegryMonitorDataGen.next();
                Put put = new Put(Bytes.toBytes(record.userId + "_" +
                        (Long.MAX_VALUE - record.time)));

                put.addColumn(HBaseConstants.columnFamily,
                        HBaseConstants.column, Bytes.toBytes(record.usedAmount));

                bufferedMutator.mutate(put);

                if (i % 1000 == 0) {
                    Thread.sleep(waitTimeEvery1000);
                }
            }
            bufferedMutator.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static void megaScan(Connection connection, String tableStr) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableStr));
        Scan scan = new Scan();
        scan.setBatch(1000);
        scan.setCaching(1000);
        scan.setCacheBlocks(false);

        ResultScanner scanner = table.getScanner(scan);

        Iterator<Result> it = scanner.iterator();
        while(it.hasNext()) {
            Result result = it.next();
            System.err.println(" - " + Bytes.toString(result.getRow()) + ":" +
                    Bytes.toString(result.getValue(HBaseConstants.columnFamily,
                            HBaseConstants.column)));
        }
    }

}
