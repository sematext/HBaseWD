/*
 * Copyright 2010 Sematext International
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sematext.hbase.wd;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Defines the way row keys are distributed
 *
 * @author Alex Baranau
 */
public abstract class AbstractRowKeyDistributor implements Parametrizable {
  public abstract byte[] getDistributedKey(byte[] originalKey);

  public abstract byte[] getOriginalKey(byte[] adjustedKey);

  public abstract byte[][] getAllDistributedKeys(byte[] originalKey);

  /**
   * Gets all distributed intervals based on the original start & stop keys.
   * Used when scanning all buckets based on start/stop row keys. Should return keys so that all buckets in which
   * records between originalStartKey and originalStopKey were distributed are "covered".
   * @param originalStartKey start key
   * @param originalStopKey stop key
   * @return array[Pair(startKey, stopKey)]
   */
  public Pair<byte[], byte[]>[] getDistributedIntervals(byte[] originalStartKey, byte[] originalStopKey) {
    byte[][] startKeys = getAllDistributedKeys(originalStartKey);
    byte[][] stopKeys;

    if (Arrays.equals(originalStopKey, HConstants.EMPTY_END_ROW)) {
      // Stop keys are one more than each prefix.
      stopKeys = getAllDistributedKeys(HConstants.EMPTY_BYTE_ARRAY);
      for (int i = 0; i < stopKeys.length; i++) {
        byte[] stopKey = stopKeys[i];
        boolean carry = false;
        int increment = 1;
        for (int j = stopKey.length - 1; j >= 0; --j) {
          int stopKeyByteInt = stopKey[j];
           stopKeyByteInt += increment;
           increment = 0;
          if (carry) {
            ++stopKeyByteInt;
          }
          if (stopKeyByteInt >= 256) {
            stopKey[j] = (byte)(stopKeyByteInt % 256);
            carry = true;
          } else {
            stopKey[j] = (byte)stopKeyByteInt;
            carry = false;
          }
        }
      }
    } else {
      stopKeys = getAllDistributedKeys(originalStopKey);
      assert stopKeys.length == startKeys.length;
    }

    Pair<byte[], byte[]>[] intervals = new Pair[startKeys.length];
    for (int i = 0; i < startKeys.length; i++) {
      intervals[i] = new Pair<>(startKeys[i], stopKeys[i]);
    }

    return intervals;
  }

  public final Scan[] getDistributedScans(Scan original) throws IOException {
    Pair<byte[], byte[]>[] intervals = getDistributedIntervals(original.getStartRow(), original.getStopRow());

    Scan[] scans = new Scan[intervals.length];
    for (int i = 0; i < intervals.length; i++) {
      scans[i] = new Scan(original);
      scans[i].setStartRow(intervals[i].getFirst());
      scans[i].setStopRow(intervals[i].getSecond());
    }
    return scans;
  }

  public void addInfo(Configuration conf) {
    conf.set(WdTableInputFormat.ROW_KEY_DISTRIBUTOR_CLASS, this.getClass().getCanonicalName());
    String paramsToStore = getParamsToStore();
    if (paramsToStore != null) {
      conf.set(WdTableInputFormat.ROW_KEY_DISTRIBUTOR_PARAMS, paramsToStore);
    }
  }
}
