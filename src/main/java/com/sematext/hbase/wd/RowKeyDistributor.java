package com.sematext.hbase.wd;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;

public interface RowKeyDistributor extends Parametrizable {

	public byte[] getDistributedKey(byte[] originalKey);

	public byte[] getOriginalKey(byte[] adjustedKey);

	public byte[][] getAllDistributedKeys(byte[] originalKey);

	/**
	 * Gets all distributed intervals based on the original start & stop keys.
	 * Used when scanning all buckets based on start/stop row keys. Should return keys so that all buckets in which
	 * records between originalStartKey and originalStopKey were distributed are "covered".
	 * @param originalStartKey start key
	 * @param originalStopKey stop key
	 * @return array[Pair(startKey, stopKey)]
	 */
	public Pair<byte[], byte[]>[] getDistributedIntervals(
			byte[] originalStartKey, byte[] originalStopKey);

	public Scan[] getDistributedScans(Scan original) throws IOException;

}