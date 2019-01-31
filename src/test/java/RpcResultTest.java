import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class RpcResultTest extends BaseTest{
    private static final String CF1 = "CF1";
    private static final String CF2 = "CF2";

    @BeforeClass
    public static void dump() throws IOException {
        TableName tn = TableName.valueOf(TABLE);
        try(Admin admin = connection.getAdmin()){
            admin.createTable(TableDescriptorBuilder
                    .newBuilder(tn)
                    .setColumnFamilies(Arrays.asList(
                            ColumnFamilyDescriptorBuilder.newBuilder(CF1.getBytes()).build(),
                            ColumnFamilyDescriptorBuilder.newBuilder(CF2.getBytes()).build()
                    ))
                    .build());
        }

        // 每个列族10列
        List<Put> puts = new ArrayList<>();
        for (int j = 0; j < 10; j++) {
            String rowkey = "row" + j;
            for (int i = 0; i < 10; i++) {
                String qualifier = "Q" + i;
                String value = "VALUE" + i;
                puts.add(new Put(rowkey.getBytes())
                        .addColumn(CF1.getBytes(), qualifier.getBytes(), value.getBytes())
                        .addColumn(CF2.getBytes(), qualifier.getBytes(), value.getBytes()));
            }
        }
        try (Table table = connection.getTable(tn)){
            table.put(puts);
        }
    }

    public void scan(int caching, int batch) throws IOException {
        Logger logger = Logger.getLogger("org.apache.hadoop");
        final int[] counters = {0, 0};

        // set logger
        logger.removeAllAppenders();
        logger.setAdditivity(false);
        logger.addAppender(new AppenderSkeleton() {
            @Override
            protected void append(LoggingEvent loggingEvent) {
                String msg = loggingEvent.getMessage().toString();
                if (StringUtils.isNotEmpty(msg) && msg.contains("Call: next")){
                    counters[0]++;
                }
            }

            @Override
            public void close() {

            }

            @Override
            public boolean requiresLayout() {
                return false;
            }
        });
        logger.setLevel(Level.DEBUG);
        Scan scan = new Scan()
                .setCaching(caching)
                .setBatch(batch);
        ResultScanner scanner = null;
        try (Table table = connection.getTable(TableName.valueOf(TABLE))) {
            scanner = table.getScanner(scan);
            for (Result result : scanner){
                counters[1]++;
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
        System.out.println((String.format("Caching: {%d}, Batch: {%d}, Rpc: {%d}, Results: {%d}", caching, batch, counters[0], counters[1])));
    }

    @Test
    public void rpcTest() throws IOException {
        scan(1, 1);
        scan(200, 1);
        scan(2000, 100);
        scan(2, 100);
        scan(2, 10);
        scan(5, 100);
        scan(5, 20);
        scan(10, 10);
    }
}
