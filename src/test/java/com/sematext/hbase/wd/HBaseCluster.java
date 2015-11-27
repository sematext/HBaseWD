package com.sematext.hbase.wd;

import com.github.sakserv.minicluster.impl.HbaseLocalCluster;
import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.List;

/**
 * @author shashank.g
 */
public class HBaseCluster {

    private static final int ZK_PORT = 12346;
    private ZookeeperLocalCluster zookeeperLocalCluster;
    private HbaseLocalCluster hbaseLocalCluster;
    private Configuration configuration = new Configuration();
    private Connection connection;
    private Admin admin;

    /**
     * Start hbase cluster
     *
     * @throws Exception
     */
    public void start() throws Exception {
        zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
                .setPort(ZK_PORT)
                .setTempDir("embedded_zookeeper")
                .setZookeeperConnectionString("localhost:" + ZK_PORT)
                .build();

        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(25112)
                .setHbaseMasterInfoPort(-1)
                .setNumRegionServers(1)
                .setHbaseRootDir("embedded_hbase")
                .setZookeeperPort(ZK_PORT)
                .setZookeeperConnectionString("localhost:" + ZK_PORT)
                .setZookeeperZnodeParent("/hbase-unsecure")
                .setHbaseWalReplicationEnabled(false)
                .setHbaseConfiguration(configuration)
                .build();

        zookeeperLocalCluster.start();
        hbaseLocalCluster.start();
        createConnection();
    }

    /**
     * Stops hbase cluster
     *
     * @throws Exception
     */
    public void stop() throws Exception {
        hbaseLocalCluster.stop();
        zookeeperLocalCluster.stop();
    }

    /**
     * Get configuration
     *
     * @return configuration
     */
    public Configuration getConfiguration() {
        return configuration;
    }

    /**
     * Creates connection
     *
     * @throws IOException
     */
    private void createConnection() throws IOException {
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
    }

    /**
     * Returns the table object for given table name
     *
     * @param table string
     * @return table object
     * @throws Exception
     */
    public synchronized Table getTable(final String table) throws Exception {
        return connection.getTable(TableName.valueOf(table));
    }

    /**
     * Create hbase table
     *
     * @param tableName      to be created
     * @param columnFamilies to be created
     * @throws IOException
     */
    public String createTable(final String tableName, final List<byte[]> columnFamilies) throws Exception {
        final TableName tableToCreate = TableName.valueOf(tableName);
        final HTableDescriptor tableDescriptor = new HTableDescriptor(tableToCreate);
        for (final byte[] columnFamily : columnFamilies) {
            final HColumnDescriptor descriptor = new HColumnDescriptor(columnFamily);
            descriptor.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
            tableDescriptor.addFamily(descriptor);
        }
        admin.createTable(tableDescriptor);

        return tableName;
    }
}
