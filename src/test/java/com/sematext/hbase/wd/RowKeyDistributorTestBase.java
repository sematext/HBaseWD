package com.sematext.hbase.wd;

import com.beust.jcommander.internal.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * @author shashank.g
 */
public class RowKeyDistributorTestBase {

    protected static final byte[] CF = Bytes.toBytes("c");
    protected static final byte[] QUAL = Bytes.toBytes("q");
    private AbstractRowKeyDistributor keyDistributor;
    private HBaseCluster cluster;

    public RowKeyDistributorTestBase(AbstractRowKeyDistributor keyDistributor) {
        this.keyDistributor = keyDistributor;
    }

    @BeforeClass
    public void setup() throws Exception {
        cluster = new HBaseCluster();
        cluster.start();
    }

    @AfterClass
    public void teardown() throws Exception {
        cluster.stop();
    }

    /**
     * Testing simple get.
     */
    @Test
    public void testGet() throws Exception {
        final String tableName = cluster.createTable(getTableName(), Lists.newArrayList(CF));

        // Testing simple get
        byte[] key = new byte[]{123, 124, 122};
        byte[] distributedKey = keyDistributor.getDistributedKey(key);
        byte[] value = Bytes.toBytes("some");

        cluster.getTable(tableName).put(new Put(distributedKey).add(CF, QUAL, value));

        Result result = cluster.getTable(tableName).get(new Get(distributedKey));
        Assert.assertEquals(key, keyDistributor.getOriginalKey(result.getRow()));
        Assert.assertEquals(value, result.getValue(CF, QUAL));
    }

    /**
     * Test scan with start and stop key.
     */
    @Test
    public void testSimpleScanBounded() throws Exception {
        final String tableName = cluster.createTable(getTableName(), Lists.newArrayList(CF));
        long origKeyPrefix = System.currentTimeMillis();

        int seekIntervalMinValue = 100;
        int seekIntervalMaxValue = 899;
        byte[] startKey = Bytes.toBytes(origKeyPrefix + seekIntervalMinValue);
        byte[] stopKey = Bytes.toBytes(origKeyPrefix + seekIntervalMaxValue + 1);
        Scan scan = new Scan(startKey, stopKey);
        testSimpleScanInternal(tableName, origKeyPrefix, scan, 500, 500, seekIntervalMinValue, seekIntervalMaxValue);
    }

    /**
     * Test scan over the whole table.
     */
    @Test
    public void testSimpleScanUnbounded() throws Exception {
        final String tableName = cluster.createTable(getTableName(), Lists.newArrayList(CF));
        long origKeyPrefix = System.currentTimeMillis();
        testSimpleScanInternal(tableName, origKeyPrefix, new Scan(), 500, 500, 0, 999);
    }

    /**
     * Test scan without stop key.
     */
    @Test
    public void testSimpleScanWithoutStopKey() throws Exception {
        final String tableName = cluster.createTable(getTableName(), Lists.newArrayList(CF));
        long origKeyPrefix = System.currentTimeMillis();
        int seekIntervalMinValue = 100;
        byte[] startKey = Bytes.toBytes(origKeyPrefix + seekIntervalMinValue);
        testSimpleScanInternal(tableName, origKeyPrefix, new Scan(startKey), 500, 500, 100, 999);
    }

    /**
     * Test scan with start and stop key.
     */
    @Test
    public void testMapReduceBounded() throws Exception {
        final String tableName = cluster.createTable(getTableName(), Lists.newArrayList(CF));
        long origKeyPrefix = System.currentTimeMillis();
        int seekIntervalMinValue = 100;
        int seekIntervalMaxValue = 899;
        byte[] startKey = Bytes.toBytes(origKeyPrefix + seekIntervalMinValue);
        byte[] stopKey = Bytes.toBytes(origKeyPrefix + seekIntervalMaxValue + 1);
        Scan scan = new Scan(startKey, stopKey);
        testMapReduceInternal(tableName, origKeyPrefix, scan, 500, 500, seekIntervalMinValue, seekIntervalMaxValue);
    }

    /**
     * Test scan over the whole table.
     */
    @Test
    public void testMapReduceUnbounded() throws Exception {
        final String tableName = cluster.createTable(getTableName(), Lists.newArrayList(CF));
        long origKeyPrefix = System.currentTimeMillis();
        testMapReduceInternal(tableName, origKeyPrefix, new Scan(), 500, 500, 0, 999);
    }

    private int writeTestData(String tableName, long origKeyPrefix, int numRows, int rowKeySeed,
                              int seekIntervalMinValue, int seekIntervalMaxValue) throws Exception {
        int valuesCountInSeekInterval = 0;
        for (int i = 0; i < numRows; i++) {
            int val = rowKeySeed + i - i * (i % 2) * 2; // i.e. 500, 499, 502, 497, 504, ...
            valuesCountInSeekInterval += (val >= seekIntervalMinValue && val <= seekIntervalMaxValue) ? 1 : 0;
            byte[] key = Bytes.toBytes(origKeyPrefix + val);
            byte[] distributedKey = keyDistributor.getDistributedKey(key);
            byte[] value = Bytes.toBytes(val);
            cluster.getTable(tableName).put(new Put(distributedKey).add(CF, QUAL, value));
        }
        return valuesCountInSeekInterval;
    }

    private void testSimpleScanInternal(String tableName, long origKeyPrefix, Scan scan, int numValues, int startWithValue,
                                        int seekIntervalMinValue, int seekIntervalMaxValue) throws Exception {

        int valuesCountInSeekInterval = writeTestData(tableName, origKeyPrefix, numValues, startWithValue, seekIntervalMinValue, seekIntervalMaxValue);
        ResultScanner distributedScanner = DistributedScanner.create(cluster.getTable(tableName), scan, keyDistributor);

        Result previous = null;
        int countMatched = 0;
        for (Result current : distributedScanner) {
            countMatched++;
            if (previous != null) {
                byte[] currentRowOrigKey = keyDistributor.getOriginalKey(current.getRow());
                byte[] previousRowOrigKey = keyDistributor.getOriginalKey(previous.getRow());
                Assert.assertTrue(Bytes.compareTo(currentRowOrigKey, previousRowOrigKey) >= 0);

                int currentValue = Bytes.toInt(current.getValue(CF, QUAL));
                Assert.assertTrue(currentValue >= seekIntervalMinValue);
                Assert.assertTrue(currentValue <= seekIntervalMaxValue);
            }
            previous = current;
        }
        Assert.assertEquals(valuesCountInSeekInterval, countMatched);
    }

    private void testMapReduceInternal(String tableName, long origKeyPrefix, Scan scan, int numValues, int startWithValue,
                                       int seekIntervalMinValue, int seekIntervalMaxValue) throws Exception {
        int valuesCountInSeekInterval = writeTestData(tableName, origKeyPrefix, numValues, startWithValue,
                seekIntervalMinValue, seekIntervalMaxValue);

        // Reading data
        Configuration conf = cluster.getConfiguration();
        Job job = new Job(conf, "testMapReduceInternal()-Job");
        job.setJarByClass(this.getClass());
        TableMapReduceUtil.initTableMapperJob(tableName, scan,
                RowCounterMapper.class, ImmutableBytesWritable.class, Result.class, job);

        // Substituting standard TableInputFormat which was set in TableMapReduceUtil.initTableMapperJob(...)
        job.setInputFormatClass(WdTableInputFormat.class);
        keyDistributor.addInfo(job.getConfiguration());

        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(0);

        boolean succeeded = job.waitForCompletion(true);
        Assert.assertTrue(succeeded);

        long mapInputRecords = job.getCounters().findCounter(RowCounterMapper.Counters.ROWS).getValue();
        Assert.assertEquals(valuesCountInSeekInterval, mapInputRecords);
    }

    /**
     * Mapper that runs the count.
     * NOTE: it was copied from RowCounter class
     */
    static class RowCounterMapper extends TableMapper<ImmutableBytesWritable, Result> {
        /**
         * Counter enumeration to count the actual rows.
         */
        public static enum Counters {
            ROWS
        }

        @Override
        public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
            for (KeyValue value : values.list()) {
                if (value.getValueArray().length > 0) {
                    context.getCounter(Counters.ROWS).increment(1);
                    break;
                }
            }
        }
    }

    private String getTableName() {
        return "test" + System.currentTimeMillis();
    }
}
