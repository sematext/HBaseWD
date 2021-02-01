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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface for client-side scanning the data written with keys distribution
 *
 * @author Alex Baranau
 */
public class DistributedScanner implements ResultScanner {
  private final AbstractRowKeyDistributor keyDistributor;
  private final ScannerWorker[] scannerWorkers;
  private Result next = null;
  // TODO: Perhaps this should be tuneable.
  private static final int RESULT_SLOTS = 5;
  Logger logger = LoggerFactory.getLogger(DistributedScanner.class);

  private static class ScannerWorker {
    // This is an object to hold pending result(s) from a scanner.
    // The caller will have a collection of these, which it will iterate
    // to find the next record to return from all the available scanners.
    // This basically implements the Dijkstra producer-consumer algo.
    Result[] results = new Result[RESULT_SLOTS];
    Semaphore readable = new Semaphore(0);
    Semaphore writable = new Semaphore(RESULT_SLOTS);
    int nextReadSlot = 0;
    int nextWriteSlot = 0;
    // eof is set when the scanner is empty, but there may still be results to process in the results buffer
    boolean eof = false;
    // exhausted is only used by the caller, and it indicates that there is nothing more to do with this worker
    boolean exhausted = false;
    final private ExecutorService executor = Executors.newSingleThreadExecutor();
    public int workerReadCount = 0;
    public int callerReadCount = 0;
    private ResultScanner scanner = null;

    public ScannerWorker(int instanceNumber, Table table, Scan scan) {
      executor.submit(new ScannerResultCollector(table, scan));
    }

    public void shutdown() {
      if (scanner != null) {
        scanner.close();
      }
      executor.shutdown();
    }

    private class ScannerResultCollector implements Callable<Void> {
      private final Table table;
      private final Scan scan;
      private ScannerResultCollector(Table table, Scan scan) {
        this.table = table;
        this.scan = scan;
      }

      @Override
      public Void call() throws IOException, InterruptedException {
        try {
          scanner = table.getScanner(scan);
          Result result;
          while ((result = scanner.next()) != null) {
            // logger.debug("worker {} writing row {}", instanceNumber, result.getRow().toString());
            ++workerReadCount;
            writable.acquire();
            results[nextWriteSlot] = result;
            nextWriteSlot = (nextWriteSlot + 1) % RESULT_SLOTS;
            readable.release();
          }
        } finally {
          if (scanner != null) {
            scanner.close();
            scanner = null;
          }
        }
        eof = true;
        return null;
      }
    }
  }

  public DistributedScanner(Table table, Scan originalScan, AbstractRowKeyDistributor keyDistributor) throws IOException {
    Scan[] scans = keyDistributor.getDistributedScans(originalScan);
    this.keyDistributor = keyDistributor;
    this.scannerWorkers = new ScannerWorker[scans.length];
    for (int i = 0; i < this.scannerWorkers.length; i++) {
      this.scannerWorkers[i] = new ScannerWorker(i, table, scans[i]);
    }
  }

  private boolean hasNext() throws InterruptedException {
    if (next != null) {
      return true;
    }

    next = nextInternal();

    return next != null;
  }

  @Override
  public Result next() throws IOException {
    try {
      if (hasNext()) {
        Result toReturn = next;
        next = null;
        return toReturn;
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    return null;
  }

  @Override
  public Result[] next(int nbRows) throws IOException {
    // Identical to HTable.ClientScanner implementation
    // Collect values to be returned here
    ArrayList<Result> resultSets = new ArrayList<>(nbRows);
    for(int i = 0; i < nbRows; i++) {
      Result next = next();
      if (next != null) {
        resultSets.add(next);
      } else {
        break;
      }
    }
    return resultSets.toArray(new Result[0]);
  }

  @Override
  public void close() {
    for (int i = 0; i < scannerWorkers.length; i++) {
      scannerWorkers[i].shutdown();
    }
  }

  @Override
  public boolean renewLease() {
    return false;
  }

  @Override
  public ScanMetrics getScanMetrics() {
    return null;
  }

  // This is the historical factory, but you should also be able to use the constructor these days.
  public static DistributedScanner create(Table table, Scan originalScan, AbstractRowKeyDistributor keyDistributor) throws IOException {
    return new DistributedScanner(table, originalScan, keyDistributor);
  }

  private Result nextInternal() throws InterruptedException {
    Result result = null;
    int indexOfScannerToUse = -1;
    SCANNER_RESULTS_LOOP: for (int i = 0; i < scannerWorkers.length; i++) {
      if (scannerWorkers[i].exhausted == true) {
        continue;
      }

      while (scannerWorkers[i].readable.tryAcquire() == false) {
        if (scannerWorkers[i].eof == true) {
          scannerWorkers[i].exhausted = true;
          continue SCANNER_RESULTS_LOOP;
        } else {
          // No current results to read, but not eof yet.  Nap and try again.
          // Note that we really can't proceed in this case since the next row
          // from this scanner might be the lexically next row we need for this
          // result set.
          // This usually only happens in 2 places:
          // 1) at the very beginning of scanning while we're still waiting for
          //    scanners to open
          // 2) at the end of the scan, if the thread isn't able to set eof fast enough.
          // There's a chance that you can hit this in the middle of a scan,
          // but that is only if HBase isn't able to keep up with your client scans.
          // There's nothing we can really do about that in this codebase.  :)
          logger.info("sleeping for worker {} after {} reads", i, scannerWorkers[i].callerReadCount);
          Thread.sleep(1);
        }
      }

      // if result is null or next record has original key less than the candidate to be returned
      if (result == null || Bytes.compareTo(keyDistributor.getOriginalKey(scannerWorkers[i].results[scannerWorkers[i].nextReadSlot].getRow()),
                                            keyDistributor.getOriginalKey(result.getRow())) < 0) {
        result = scannerWorkers[i].results[scannerWorkers[i].nextReadSlot];
        indexOfScannerToUse = i;
      }

      scannerWorkers[i].readable.release();
    }

    if (indexOfScannerToUse >= 0) {
      // logger.debug("consuming key {} from worker {}", result.getRow().toString(), indexOfScannerToUse);
      scannerWorkers[indexOfScannerToUse].readable.acquire();
      scannerWorkers[indexOfScannerToUse].nextReadSlot = (scannerWorkers[indexOfScannerToUse].nextReadSlot + 1) % RESULT_SLOTS;
      scannerWorkers[indexOfScannerToUse].writable.release();

      scannerWorkers[indexOfScannerToUse].callerReadCount++;
    }

    return result;
  }

  @Override
  public Iterator<Result> iterator() {
    // Identical to HTable.ClientScanner implementation
    return new Iterator<Result>() {
      // The next RowResult, possibly pre-read
      Result next = null;

      // return true if there is another item pending, false if there isn't.
      // this method is where the actual advancing takes place, but you need
      // to call next() to consume it. hasNext() will only advance if there
      // isn't a pending next().
      public boolean hasNext() {
        if (next == null) {
          try {
            next = DistributedScanner.this.next();
            return next != null;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        return true;
      }

      // get the pending next item and advance the iterator. returns null if
      // there is no next item.
      public Result next() {
        // since hasNext() does the real advancing, we call this to determine
        // if there is a next before proceeding.
        if (!hasNext()) {
          return null;
        }

        // if we get to here, then hasNext() has given us an item to return.
        // we want to return the item and then null out the next pointer, so
        // we use a temporary variable.
        Result temp = next;
        next = null;
        return temp;
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public void forEach(Consumer<? super Result> action) {

  }

  @Override
  public Spliterator<Result> spliterator() {
    return null;
  }
}
