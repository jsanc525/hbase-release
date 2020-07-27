package org.apache.hadoop.hbase.mob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
/**
 *  This test class validates no MOB data loss for
 *  any snapshot operation i.e clone/restore
 */
@Category(MediumTests.class)
public class TestMOBRestoreSnapshot {

  private HBaseTestingUtility HTU;
  private Configuration conf;
  private HTableDescriptor hdt;

  @Before
  public void setUp() throws Exception {
    HTU = new HBaseTestingUtility();
    hdt = HTU.createTableDescriptor("testMobCompactTable");
    conf = HTU.getConfiguration();
    initConf();
    HTU.startMiniCluster();
  }

  private void initConf() {
    conf.setInt("hfile.format.version", 3);
    conf.setLong(MobConstants.MOB_MINIMUM_FILE_AGE_TO_ARCHIVE_KEY, 0);
    conf.setLong(TimeToLiveHFileCleaner.TTL_CONF_KEY, 0);
  }

  @After
  public void tearDown() throws Exception {
    HTU.shutdownMiniCluster();
  }

  @Test public void testMOBDataLossPostSnapshotRestore() throws IOException, InterruptedException {
    final Admin admin = HTU.getHBaseAdmin();
    final String famStr = "f1";
    final byte[] fam = Bytes.toBytes(famStr);
    final byte[] qualifier = Bytes.toBytes("q1");
    final long mobLen = 10;
    final byte[] mobVal = Bytes.toBytes("01234567890123456789");
    final HColumnDescriptor hcd = new HColumnDescriptor(fam);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(mobLen);
    hcd.setMaxVersions(1);
    hdt.addFamily(hcd);
    TableName tableName = hdt.getTableName();
    String snapshotName = "s";
    Table table = HTU.createTable(hdt, null);
    Put put = new Put(Bytes.toBytes("1"));
    put.addColumn(fam, qualifier, mobVal);
    table.put(put);
    admin.flush(tableName);
    put = new Put(Bytes.toBytes("2"));
    put.addColumn(fam, qualifier, mobVal);
    table.put(put);
    admin.flush(tableName);
    admin.snapshot(snapshotName,tableName);
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    admin.restoreSnapshot(snapshotName);
    admin.deleteSnapshot(snapshotName);
    MobUtils.cleanupObsoleteMobFiles(conf, tableName);
    Thread.sleep(3000l);
    HTU.getMiniHBaseCluster().getMaster().getHFileCleaner().choreForTesting();
    Thread.sleep(3000l);
    Result result = table.getScanner(new Scan(Bytes.toBytes("1"))).next();
    table.close();
    Assert.assertTrue(result != null);
  }

}
