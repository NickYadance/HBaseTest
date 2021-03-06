import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseTest extends BaseTest{
    public static Logger logger = LoggerFactory.getLogger(HBaseTest.class);

	@BeforeClass
	public static void dump(){
		// 建表
		try(Admin admin = connection.getAdmin()){
			TableName tableName = TableName.valueOf(TABLE);
			if (admin.tableExists(tableName)){
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}
			HTableDescriptor descriptor = new HTableDescriptor(tableName)
					.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
			admin.createTable(descriptor, toByteArray(splitPoints));
			List<HRegionInfo> regionInfos = admin.getTableRegions(tableName);
			for (HRegionInfo info : regionInfos) {
						logger.info("start key: {}, end key: {}, name: {}", new String(info.getStartKey()), new String(info.getEndKey()), info.getRegionNameAsString());
			}

		} catch (Exception e) {
			logger.error("create table error: {}", e);
			return;
		}

		// 插入100000条14位测试数据
		int nums = 100000;
		try (BufferedMutator bufferedMutator = connection.getBufferedMutator(TableName.valueOf(TABLE))) {
			long rangeHigh = 10000000000000L;
			for (int i = 0; i < nums; i ++) {
				long uid = (long) (Math.random() * rangeHigh);
				bufferedMutator.mutate(
						new Put(Long.toString(uid).getBytes()).addColumn(COLUMN_FAMILY.getBytes(), QUALIFIER.getBytes(), VALUE.getBytes())
				);
			}
		} catch (Exception e){
			logger.error("dump data error: {}", e);
			return;
		}

		return;
	}

	private static byte[][] toByteArray(String[] stringArray){
		byte[][] bytes = new byte[stringArray.length][];
		for (int i = 0; i < stringArray.length; i++) {
			bytes[i] = stringArray[i].getBytes();
		}
		return bytes;
	}

	@Test
	public void basic(){

	}

	@Test
	public void MultiRegionTest(){
		// 扫描{"111", "444"}
		String startRow = splitPoints[1];
		String stopRow = splitPoints[4];

		// 不做分页过滤
		// 对Region做count
		long count = doScan(new Scan()
				.withStartRow(startRow.getBytes())
				.withStopRow(stopRow.getBytes())
				.addColumn(COLUMN_FAMILY.getBytes(), QUALIFIER.getBytes())).size();

		// 做分页过滤
		// 与count对比
		Assert.assertEquals(doTest(startRow, stopRow, count), count);
	}

	@Test
	public void SingleRegionTest1(){
		// 扫描{"111", "222"}
		String startRow = splitPoints[1];
		String stopRow = splitPoints[2];

		// 不做分页过滤
		// 对Region做count
		long count = doScan(new Scan()
				.withStartRow(startRow.getBytes())
				.withStopRow(stopRow.getBytes())
				.addColumn(COLUMN_FAMILY.getBytes(), QUALIFIER.getBytes())).size();

		// 做分页过滤
		// 与count对比
		Assert.assertEquals(doTest(startRow, stopRow, count), count);
	}

	@Test
	public void SingleRegionTest2(){
		// 扫描{"222", "333"}
		String startRow = splitPoints[2];
		String stopRow = splitPoints[3];

		// 不做分页过滤
		// 对Region做count
		long count = doScan(new Scan()
				.withStartRow(startRow.getBytes())
				.withStopRow(stopRow.getBytes())
				.addColumn(COLUMN_FAMILY.getBytes(), QUALIFIER.getBytes())).size();

		// 做分页过滤
		// 与count对比
		Assert.assertEquals(doTest(startRow, stopRow, count), count);
	}

	@Test
	public void SingleRegionTest3(){
		// 扫描{"333", "444"}
		String startRow = splitPoints[3];
		String stopRow = splitPoints[4];

		// 不做分页过滤
		// 对Region做count
		long count = doScan(new Scan()
				.withStartRow(startRow.getBytes())
				.withStopRow(stopRow.getBytes())
				.addColumn(COLUMN_FAMILY.getBytes(), QUALIFIER.getBytes())).size();

		// 做分页过滤
		// 与count对比
		Assert.assertEquals(doTest(startRow, stopRow, count), count);
	}


	private long doTest(String startRow, String stopRow, long expectedRow){
		long sum = 0L;
		byte[] start = startRow.isEmpty()? HConstants.EMPTY_START_ROW : startRow.getBytes();
		byte[] stop = stopRow.isEmpty() ? HConstants.EMPTY_END_ROW : stopRow.getBytes();
		logger.info("Test: region from [{}] to [{}], started", startRow, stopRow);
		try {
			byte[] postfix = new byte[]{0x00};
			List<String> resultList;
			while (true) {
				long tStart = System.currentTimeMillis();
				Scan scan = new Scan()
						.withStartRow(start)
						.withStopRow(stop)
						.addColumn(COLUMN_FAMILY.getBytes(), QUALIFIER.getBytes())
						.setCaching(CACHING)
						.setFilter(new PageFilter(PAGE_SZE));
				resultList = doScan(scan);
				if (resultList == null || resultList.isEmpty()) {
					break;
				}
				if (resultList.size() > PAGE_SZE){
					logger.warn("**************** region skiped here ****************");
				}
				String firstRowkey = resultList.get(0);
				String lastRowkey = resultList.get(resultList.size() - 1);
				sum += resultList.size();
				long tEnd = System.currentTimeMillis();
				logger.info("Test: results from [{}] to [{}], sum [{} : {}], cost: [{}ms]", firstRowkey, lastRowkey, resultList.size(), sum, tEnd - tStart);
				start = Bytes.add(lastRowkey.getBytes(), postfix);
			}
		} catch (Exception e){
			logger.error("Test: failed: {}", e);
		}
		logger.info("Test: region from [{}] to [{}], sum [{}], expect [{}], missed [{}]", startRow, stopRow, sum, expectedRow, expectedRow - sum);
		return sum;
	}

	private List<String> doScan(Scan scan){
		List<String> resultList = new ArrayList<>();
		try(Table table = connection.getTable(TableName.valueOf(TABLE))){
			ResultScanner resultScanner = table.getScanner(scan);
			Result result;
			while ( (result = resultScanner.next()) != null){
				resultList.add(new String(result.getRow()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return resultList;
	}
}
