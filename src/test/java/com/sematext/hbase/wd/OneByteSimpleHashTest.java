/*
 * Copyright (c) Sematext International
 * All Rights Reserved
 * <p/>
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF Sematext International
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 */
package com.sematext.hbase.wd;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OneByteSimpleHashTest {
  @Test
  public void testMaxDistribution() {
    RowKeyDistributorByHashPrefix.OneByteSimpleHash hasher = new RowKeyDistributorByHashPrefix.OneByteSimpleHash(256);
    byte[][] allPrefixes = hasher.getAllPossiblePrefixes();
    assertEquals(256, allPrefixes.length);
    Random r = new Random();
    for (int i = 0; i < 1000; i++) {
      byte[] originalKey = new byte[3];
      r.nextBytes(originalKey);
      byte[] hash = hasher.getHashPrefix(originalKey);
      boolean found = false;
      for (int k = 0; k < allPrefixes.length; k++) {
        if (Arrays.equals(allPrefixes[k], hash)) {
          found = true;
          break;
        }
      }
      assertTrue(found, "Hashed prefix wasn't found in all possible prefixes, val: " + Arrays.toString(hash));
    }

    assertArrayEquals(
            hasher.getHashPrefix(new byte[] {123, 12, 11}), hasher.getHashPrefix(new byte[] {123, 12, 11}));
  }

  @Test
  public void testLimitedDistribution() {
    RowKeyDistributorByHashPrefix.OneByteSimpleHash hasher = new RowKeyDistributorByHashPrefix.OneByteSimpleHash(10);
    byte[][] allPrefixes = hasher.getAllPossiblePrefixes();
    assertEquals(10, allPrefixes.length);
    Random r = new Random();
    for (int i = 0; i < 1000; i++) {
      byte[] originalKey = new byte[3];
      r.nextBytes(originalKey);
      byte[] hash = hasher.getHashPrefix(originalKey);
      boolean found = false;
      for (int k = 0; k < allPrefixes.length; k++) {
        if (Arrays.equals(allPrefixes[k], hash)) {
          found = true;
          break;
        }
      }
      assertTrue(found, "Hashed prefix wasn't found in all possible prefixes, val: " + Arrays.toString(hash));
    }

    assertArrayEquals(
            hasher.getHashPrefix(new byte[] {123, 12, 11}), hasher.getHashPrefix(new byte[] {123, 12, 11}));
  }

  /**
   * Tests that records are well spread over buckets.
   * Note: the prefix is chosen from a set of 256 possible prefixes, "mod buckets".
   * For us to have an even distribution over the buckets, you should be choosing a power-of-2 for the number of buckets.
   * Otherwise, you will skew towards some subset of the lower-numbered buckets.
   * Specifically, a non-power-of-2 number of buckets will have a quantizing effect as, with evenly-distributed
   * inputs, each bucket will consist of inputs from x hashes, but some will end up with inputs from x+1 hashes,
   * and that can be a significantly different number of inputs.
   * For example, consider a simple case with 3 buckets and a trivial hashing algorithm
   * (hash is just the last byte of the input):
   * input    hash    bucket
   * 0x0000   0x00    0
   * 0x0001   0x01    1
   * 0x0002   0x02    2
   * 0x0003   0x03    0
   * ...
   * 0x1204   0x04    1
   * If we have 65536 inputs, 1 for each value to 64k, the number of items in each bucket turns out to be:
   * for i in {0..65535} ; do echo bucket$(( $(( $i & 0xff )) % 3 )) ; done | sort | uniq -c
   * 22016 bucket0
   * 21760 bucket1
   * 21760 bucket2
   * This is because the inputs are not distributed into the buckets, the hashes are, and not every hash represents
   * the same number of inputs.  Specifically, for a hash of 256 values, (256 % numBuckets) tells you how many buckets
   * will have an extra set of inputs in them.
   * In general, the problem gets worse as the data is spread more thinly.  At 230 buckets,
   * some buckets have 2x the number of items as the others.
   * TL;DR: choose your buckets as a power of 2.
   *
   */
  @Test
  public void testHashPrefixDistribution() {
    testDistribution(16, 100);
    testDistribution(24, 100); // Not advised
    testDistribution(32, 100);
    testDistribution(64, 100);
    testDistribution(128, 100);
    testDistribution(256, 100);
  }

  private void testDistribution(int maxBuckets, int countForEachBucket) {
    System.out.println("Testing maxBuckets=" + maxBuckets + ", countForEachBucket=" + countForEachBucket);
    RowKeyDistributorByHashPrefix distributor = new RowKeyDistributorByHashPrefix(new RowKeyDistributorByHashPrefix.OneByteSimpleHash(maxBuckets));
    int[] bucketCounts = new int[maxBuckets];
    for (int i = 0; i < maxBuckets * countForEachBucket; i++) {
      byte[] original = Bytes.toBytes(i);
      byte[] distributed = distributor.getDistributedKey(original);
      bucketCounts[distributed[0] & 0xff]++;
    }

    for (int i = 0; i < bucketCounts.length; ++i) {
      System.out.println("Bucket " + i + " contains " + bucketCounts[i] + " items.");
    }

    byte[][] allKeys = distributor.getAllDistributedKeys(new byte[0]);
    assertEquals(maxBuckets, allKeys.length);

    for (int bucketCount : bucketCounts) {
      // i.e. all buckets expected to have similar amount of values (+- 10%)
      assertTrue(Math.abs((countForEachBucket - bucketCount) / (double)countForEachBucket) < 0.10,
              "Unexpected values count of " + bucketCount + " instead of (roughly) " + countForEachBucket);
    }
  }

  // This is a simple function to generate a randomized list of ints for the permutationArray in RowKeyDistributorByHashPrefix.
  // Uncomment the @Test to be able to run it to generate a new list, if needed.
  // @Test
  public void shuffleInts() {
    LinkedList<String> intList = new LinkedList<>();
    for (int i: IntStream.range(0, 256).toArray()) {
      intList.add(i, Integer.toString(i));
    }
    Collections.shuffle(intList);
    System.out.println("{" + String.join(", ",  intList) + "}");
  }
}
