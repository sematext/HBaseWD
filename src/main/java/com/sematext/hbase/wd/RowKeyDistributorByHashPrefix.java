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

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Provides handy methods to distribute
 *
 * @author Alex Baranau
 */
public class RowKeyDistributorByHashPrefix extends AbstractRowKeyDistributor {
  private static final String DELIM = "--";
  private Hasher hasher;

  /** Constructor reflection. DO NOT USE */
  public RowKeyDistributorByHashPrefix() {
  }

  public RowKeyDistributorByHashPrefix(Hasher hasher) {
    this.hasher = hasher;
  }

  public interface Hasher extends Parametrizable {
    byte[] getHashPrefix(byte[] originalKey);
    byte[][] getAllPossiblePrefixes();
    int getPrefixLength(byte[] adjustedKey);
  }

  public static class OneByteSimpleHash implements Hasher {
    private int mod;

    /**
     * For reflection, do NOT use it.
     */
    public OneByteSimpleHash() {}

    /**
     * Creates a new instance of this class.
     * @param maxBuckets max buckets number, should be in 1...255 range
     */
    public OneByteSimpleHash(int maxBuckets) {
      if (maxBuckets < 1 || maxBuckets > 256) {
        throw new IllegalArgumentException("maxBuckets should be in 1..256 range");
      }
      // i.e. "real" maxBuckets value = maxBuckets or maxBuckets-1
      this.mod = maxBuckets;
    }

    // Used to minimize # of created object instances
    // Should not be changed. TODO: secure that
    private static final byte[][] PREFIXES;

    static {
      PREFIXES = new byte[256][];
      for (int i = 0; i < 256; i++) {
        PREFIXES[i] = new byte[]{(byte) i};
      }
    }

    @Override
    public byte[] getHashPrefix(byte[] originalKey) {
      int hash = Math.abs(hashBytes(originalKey));
      return PREFIXES[hash % mod];
    }

    @Override
    public byte[][] getAllPossiblePrefixes() {
      return Arrays.copyOfRange(PREFIXES, 0, mod);
    }

    @Override
    public int getPrefixLength(byte[] adjustedKey) {
      // They're all the same length--return length of a representative one.
      return PREFIXES[0].length;
    }

    @Override
    public String getParamsToStore() {
      return String.valueOf(mod);
    }

    @Override
    public void init(String storedParams) {
      this.mod = Integer.parseInt(storedParams);
    }

    /* Compute hash for binary data. */
    /* This is Pearson hashing */
    private static final int[] permutationArray = new int[]{222, 246, 170, 127, 106, 19, 139, 248, 98, 137, 243, 6, 76, 37, 15, 240, 73, 178, 149, 67, 187, 161, 180, 229, 173, 202, 114, 1, 65, 126, 4, 238, 188, 181, 20, 138, 80, 196, 156, 185, 179, 77, 120, 43, 87, 32, 171, 221, 56, 82, 198, 247, 191, 213, 183, 131, 22, 152, 24, 45, 145, 199, 201, 224, 33, 11, 164, 130, 166, 109, 132, 184, 151, 14, 227, 49, 88, 103, 237, 252, 92, 174, 55, 255, 154, 220, 189, 101, 59, 250, 216, 249, 228, 71, 99, 241, 226, 28, 136, 242, 113, 125, 150, 190, 140, 104, 74, 119, 0, 133, 89, 111, 10, 205, 134, 58, 40, 118, 72, 144, 27, 168, 169, 5, 79, 12, 176, 42, 225, 143, 206, 209, 135, 236, 25, 200, 85, 141, 204, 115, 66, 30, 192, 232, 217, 94, 48, 91, 90, 29, 100, 31, 60, 78, 160, 231, 53, 17, 8, 34, 175, 153, 239, 21, 167, 95, 102, 129, 223, 128, 123, 207, 122, 146, 18, 233, 194, 23, 61, 148, 165, 158, 163, 69, 117, 96, 208, 210, 215, 105, 54, 16, 3, 52, 234, 121, 2, 235, 124, 253, 195, 44, 193, 13, 26, 218, 68, 47, 251, 244, 84, 219, 177, 172, 157, 35, 162, 182, 64, 7, 46, 93, 203, 70, 112, 39, 107, 57, 51, 41, 86, 36, 83, 116, 63, 108, 81, 211, 159, 110, 186, 75, 155, 214, 212, 62, 38, 142, 50, 9, 197, 245, 97, 254, 147, 230};
    private static int hashBytes(byte[] bytes) {
      int hash = permutationArray[bytes.length % 256];
      for (int i = 0; i < bytes.length; i++)
        hash = permutationArray[hash ^ ((int)bytes[i] & 0xff)];
      return hash;
    }
  }

  @Override
  public byte[] getDistributedKey(byte[] originalKey) {
    return Bytes.add(hasher.getHashPrefix(originalKey), originalKey);
  }

  @Override
  public byte[] getOriginalKey(byte[] adjustedKey) {
    int prefixLength = hasher.getPrefixLength(adjustedKey);
    if (prefixLength > 0) {
      return Bytes.tail(adjustedKey, adjustedKey.length - prefixLength);
    } else {
      return adjustedKey;
    }
  }

  @Override
  public byte[][] getAllDistributedKeys(byte[] originalKey) {
    byte[][] allPrefixes = hasher.getAllPossiblePrefixes();
    byte[][] keys = new byte[allPrefixes.length][];
    for (int i = 0; i < allPrefixes.length; i++) {
      keys[i] = Bytes.add(allPrefixes[i], originalKey);
    }

    return keys;
  }

  @Override
  public String getParamsToStore() {
    String hasherParamsToStore = hasher.getParamsToStore();
    return hasher.getClass().getName() + DELIM + (hasherParamsToStore == null ? "" : hasherParamsToStore);
  }

  @Override
  public void init(String params) {
    String[] parts = params.split(DELIM, 2);
    try {
      this.hasher = (Hasher) Class.forName(parts[0]).newInstance();
      this.hasher.init(parts[1]);
    } catch (Exception e) {
      throw new RuntimeException("RowKeyDistributor initialization failed", e);
    }
  }
}
