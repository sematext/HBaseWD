package com.sematext.hbase.wd;

public class RowKeyDistributorByHashPrefix_OneByteSimpleHashTest extends RowKeyDistributorTestBase {

    public RowKeyDistributorByHashPrefix_OneByteSimpleHashTest() {
        super(new RowKeyDistributorByHashPrefix(new RowKeyDistributorByHashPrefix.OneByteSimpleHash(15)));
    }
}
