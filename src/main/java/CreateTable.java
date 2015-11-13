import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateTable {

    public static void main(String[] args) throws IOException {

       /* if (args.length == 0) {
            System.out.println("CreateTable {tableName} {columnFamilyName} {10 or 100} {optional RegionSize}");
            return;
        }*/

        String tableName = HBaseConstants.tableName;
        byte[] columnFamilyName = HBaseConstants.columnFamily;
        String regionCount = "10";
        String zookeeper_loc = "injest-cluster-1.vpc.cloudera.com";

        long regionMaxSize = 107374182400l;

        if (args.length > 3) {
            regionMaxSize = Long.parseLong(args[3]);
        }


        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zookeeper_loc);  // Here we are running zookeeper locally

        HBaseAdmin admin = new HBaseAdmin(config);

        HTableDescriptor tableDescriptor = new HTableDescriptor();
        tableDescriptor.setName(Bytes.toBytes(tableName));

        HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamilyName);

        columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
        columnDescriptor.setBlocksize(64 * 1024);
        columnDescriptor.setBloomFilterType(BloomType.ROW);

        tableDescriptor.addFamily(columnDescriptor);

        tableDescriptor.setMaxFileSize(regionMaxSize);
        tableDescriptor.setValue(tableDescriptor.SPLIT_POLICY, ConstantSizeRegionSplitPolicy.class.getName());

        if (regionCount.equals(100)) {
            System.out.println("100 regions");
            byte[][] splitKeys = new byte[100][];
            int counter = 0;
            for (byte[] splitKey: splitKeys) {
                String key = StringUtils.leftPad(Integer.toString(counter++), 2, '0');
                splitKey = Bytes.toBytes(key);
            }

            admin.createTable(tableDescriptor, splitKeys);
        } else {
            System.out.println("10 regions");
            byte[][] splitKeys = new byte[10][];
            splitKeys[0] = Bytes.toBytes("0");
            splitKeys[1] = Bytes.toBytes("1");
            splitKeys[2] = Bytes.toBytes("2");
            splitKeys[3] = Bytes.toBytes("3");
            splitKeys[4] = Bytes.toBytes("4");
            splitKeys[5] = Bytes.toBytes("5");
            splitKeys[6] = Bytes.toBytes("6");
            splitKeys[7] = Bytes.toBytes("7");
            splitKeys[8] = Bytes.toBytes("8");
            splitKeys[9] = Bytes.toBytes("9");

            admin.createTable(tableDescriptor, splitKeys);
        }
        admin.close();
        System.out.println("Done");
    }


}
