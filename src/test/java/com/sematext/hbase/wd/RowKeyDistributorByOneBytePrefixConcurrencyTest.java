package com.sematext.hbase.wd;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RowKeyDistributorByOneBytePrefixConcurrencyTest {

    @Test
    public void checkMaxSizeConstraints() throws ExecutionException, InterruptedException {
        int maxSize = 100;
        final RowKeyDistributorByOneBytePrefix rowKeyDist = new RowKeyDistributorByOneBytePrefix((byte) maxSize);
        ExecutorService executorService = Executors.newFixedThreadPool(1000);
        List<Future<byte[]>> futures = Lists.newArrayList();
        for (int i = 0; i < 1500; i++) {
            futures.add(executorService.submit(new Callable<byte[]>() {
                @Override
                public byte[] call() throws Exception {
                    return rowKeyDist.getDistributedKey(new byte[]{});
                }
            }));
        }
        assertEquals(futures.size(), 1500);
        Map<Byte, Integer> groupBy = new HashMap<>();
        for (int i = 0; i < 1500; i++) {
            Byte key = futures.get(i).get()[0];
            if (groupBy.containsKey(key)) {
                Integer count = groupBy.get(key);
                count++;
                groupBy.put(key, count);
            } else {
                groupBy.put(key, 1);
            }

        }
        int total = 0;
        for (Integer i : groupBy.values()) {
            total += (i - 15) ^ 2;
        }
        assertTrue(Math.sqrt(total / maxSize) < 2.0);
        assertEquals(groupBy.size(), maxSize);
    }


}
