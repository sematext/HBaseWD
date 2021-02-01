package com.sematext.hbase.wd;

public class RowKeyDistributorByHashPrefix_MultiBytesPrefixHashTest extends RowKeyDistributorTestBase {
  public RowKeyDistributorByHashPrefix_MultiBytesPrefixHashTest() {
    super(new RowKeyDistributorByHashPrefix(new MultiBytesPrefixHash()));
  }

  public static class MultiBytesPrefixHash implements RowKeyDistributorByHashPrefix.Hasher {
    private static final byte[] PREFIX1 = new byte[] {(byte) 3,(byte) 23};
    private static final byte[] PREFIX2 = new byte[] {(byte) 1,(byte) 55};
    private static final byte[] PREFIX3 = new byte[] {(byte) 2,(byte) 55};
    private static final byte[][] ALL_PREFIXES = new byte[][] {PREFIX1, PREFIX2, PREFIX3};

    @Override
    public byte[] getHashPrefix(byte[] originalKey) {
      return ALL_PREFIXES[(originalKey[originalKey.length - 1] & 0xff) % 3]; // close to random prefix
    }

    @Override
    public byte[][] getAllPossiblePrefixes() {
      return ALL_PREFIXES;
    }

    @Override
    public int getPrefixLength(byte[] adjustedKey) {
      return PREFIX1.length;
    }

    @Override
    public String getParamsToStore() {
      return null;
    }

    @Override
    public void init(String storedParams) {
      // DO NOTHING
    }
  }
}
