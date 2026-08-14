/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.mapreduce;

import static org.apache.phoenix.schema.types.PDataType.TRUE_BYTES;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Progressable;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs row-level repair for a mismatched chunk by merge-scanning source and target cluster data
 * and applying targeted mutations to target.
 * <p>
 * The two scan ranges may differ: the verifier reads target over a wider range than source (covers
 * extra-on-target rows that fall between consecutive source chunks); repair must mirror the same
 * boundaries so those extras are visible here as {@code cmp > 0} rows and get deleted.
 * <p>
 * Merge-scan contract: both scanners return rows in ascending key order (HBase guarantee).
 * <ul>
 * <li>{@code cmp == 0} (same row): {@code (family, ts)}-grouped diff (see
 * {@link #generateMutationForDiffCells}); emits {@link Delete#addFamilyVersion} for target-only
 * whole slices, cell-level mutations for same-slice content drift.</li>
 * <li>{@code cmp <  0} (source-only): mirror all source cells onto target.</li>
 * <li>{@code cmp >  0} (target-only): tombstone target cells within
 * {@code [fromTime, toTime)} with {@code DeleteFamilyVersion} per {@code (cf, ts)}.</li>
 * </ul>
 * Cells outside {@code [fromTime, toTime)} are never read (scan time range), so never mutated.
 * <p>
 * Repair scan on <b>target</b> is forced {@code raw=true} + {@code readAllVersions()} regardless
 * of user flags so hidden (max-versions-filtered) target versions and target tombstone cells
 * surface directly in the merge walk. Repair scan on <b>source</b> continues to honor the user's
 * {@code --raw-scan} / {@code --read-all-versions} flags so the mirrored source view matches what
 * the verifier hashed.
 * <p>
 * Tombstone semantics: HBase has four tombstone subtypes ({@code Delete}, {@code DeleteColumn},
 * {@code DeleteFamily}, {@code DeleteFamilyVersion}). Source Puts we mirror onto target may be
 * silently shadowed by an existing target tombstone; in that case the mirror is suppressed and the
 * row carries unrepairable drift (operator must major-compact target to reap shadowing tombstones
 * before a re-run can converge). See {@link TargetRowRecord}.
 */
public final class PhoenixSyncTableChunkRepairer {

  private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixSyncTableChunkRepairer.class);

  private final Connection sourceConnection;
  private final Connection targetConnection;
  private final PTable pTable;
  private final byte[] physicalTableName;
  private final long fromTime;
  private final long toTime;
  private final boolean isRawScan;
  private final boolean isReadAllVersions;
  private final int repairBatchSize;
  private final String tableName;

  public PhoenixSyncTableChunkRepairer(Connection sourceConnection, Connection targetConnection,
    PTable pTable, byte[] physicalTableName, String tableName, long fromTime, long toTime,
    boolean isRawScan, boolean isReadAllVersions, int repairBatchSize) {
    this.sourceConnection = sourceConnection;
    this.targetConnection = targetConnection;
    this.pTable = pTable;
    this.physicalTableName = physicalTableName;
    this.tableName = tableName;
    this.fromTime = fromTime;
    this.toTime = toTime;
    this.isRawScan = isRawScan;
    this.isReadAllVersions = isReadAllVersions;
    this.repairBatchSize = repairBatchSize;
  }

  /**
   * Repairs one mismatched chunk. Returns a {@link ChunkRepairResult} carrying the terminal status
   * and accumulated {@link DriftCounters}; never throws on per-chunk scan/flush failure (returns
   * {@link ChunkRepairResult.Status#REPAIR_FAILED}). The only declared {@link SQLException}
   * surfaces from {@link Connection#unwrap}, which indicates a misconfigured connection rather than
   * a per-chunk fault.
   */
  public ChunkRepairResult repair(ChunkRepairRequest req, Progressable progress)
    throws SQLException {
    DriftCounters drift = new DriftCounters();

    LOGGER.info("Starting repair for chunk source=[{}, {}] target={}{}, {}{} on table {}",
      Bytes.toStringBinary(req.sourceStart), Bytes.toStringBinary(req.sourceEnd),
      req.targetStartInclusive ? "[" : "(", Bytes.toStringBinary(req.targetStart),
      Bytes.toStringBinary(req.targetEnd), req.targetEndInclusive ? "]" : ")", tableName);

    PhoenixConnection sourcePhoenixConn = sourceConnection.unwrap(PhoenixConnection.class);
    PhoenixConnection targetPhoenixConn = targetConnection.unwrap(PhoenixConnection.class);

    Scan sourceScan;
    Scan targetScan;
    try {
      sourceScan =
        createRepairScan(req.sourceStart, req.sourceEnd, true, true, false, sourcePhoenixConn);
      targetScan = createRepairScan(req.targetStart, req.targetEnd, req.targetStartInclusive,
        req.targetEndInclusive, true, targetPhoenixConn);
    } catch (IOException e) {
      LOGGER.error("Repair failed to build scans for chunk source=[{}, {}] on table {}: {}",
        Bytes.toStringBinary(req.sourceStart), Bytes.toStringBinary(req.sourceEnd), tableName,
        e.getMessage(), e);
      return ChunkRepairResult.failed(drift, e);
    }

    try (Table sourceHTable = sourcePhoenixConn.getQueryServices().getTable(physicalTableName);
      Table targetHTable = targetPhoenixConn.getQueryServices().getTable(physicalTableName);
      ResultScanner sourceScanner = sourceHTable.getScanner(sourceScan);
      ResultScanner targetScanner = targetHTable.getScanner(targetScan)) {
      if (req.dryRun) {
        walkAndCountDrift(sourceScanner, targetScanner, drift, progress);
      } else {
        repairDiffRows(sourceScanner, targetScanner, targetHTable, drift, progress);
      }
    } catch (IOException e) {
      // Per-chunk fault isolation. The mapper marks this chunk REPAIR_FAILED and continues
      // with the next chunk
      LOGGER.error("Repair failed for chunk source=[{}, {}] on table {}: {}",
        Bytes.toStringBinary(req.sourceStart), Bytes.toStringBinary(req.sourceEnd), tableName,
        e.getMessage(), e);
      return ChunkRepairResult.failed(drift, e);
    }

    ChunkRepairResult result = ChunkRepairResult.completed(drift);
    LOGGER.info("Completed repair for chunk source=[{}, {}] with status={}: {}",
      Bytes.toStringBinary(req.sourceStart), Bytes.toStringBinary(req.sourceEnd), result.status,
      drift.toLogString());
    return result;
  }

  /**
   * Dry-run merge-walk: bumps the three row-level drift counters and logs each diverged row; never
   * touches target. {@code rowsDifferentOnTarget} flags rows present on both sides whose contents
   * differ — verifier-only signal, not produced in repair mode (which reports cell granularity
   * instead).
   */
  private void walkAndCountDrift(ResultScanner sourceScanner, ResultScanner targetScanner,
    DriftCounters drift, Progressable progress) throws IOException {
    Result sourceResult = sourceScanner.next();
    Result targetResult = targetScanner.next();

    while (sourceResult != null || targetResult != null) {
      int cmp = compareRowKeys(sourceResult, targetResult);
      if (cmp == 0) {
        if (!rowsEqual(sourceResult, targetResult)) {
          drift.rowsDifferentOnTarget++;
          LOGGER.warn("Row different on target for table {} row={}", tableName,
            Bytes.toStringBinary(sourceResult.getRow()));
        }
        sourceResult = sourceScanner.next();
        targetResult = targetScanner.next();
      } else if (cmp < 0) {
        drift.rowsMissingOnTarget++;
        LOGGER.warn("Row missing on target for table {} row={}", tableName,
          Bytes.toStringBinary(sourceResult.getRow()));
        sourceResult = sourceScanner.next();
      } else {
        drift.rowsExtraOnTarget++;
        LOGGER.warn("Row extra on target for table {} row={}", tableName,
          Bytes.toStringBinary(targetResult.getRow()));
        targetResult = targetScanner.next();
      }
      if (progress != null) {
        progress.progress();
      }
    }
  }

  /**
   * Repair-mode merge-walk: resolves drift by emitting mutations into pending batches, flushing
   * each time the batch reaches {@link #repairBatchSize}, and finally draining the tail. Per
   * branch:
   * <ul>
   * <li>{@code cmp == 0} — {@code (family, ts)}-grouped diff (see
   * {@link #generateMutationForDiffCells}); record cell-level drift and any row-unrepairable
   * flag.</li>
   * <li>{@code cmp <  0} — mirror the source row onto target; bump {@code rowsMissing} unless the
   * whole row was shadowed, and {@code rowsCannotRepair} unless every cell was mirrored.</li>
   * <li>{@code cmp >  0} — tombstone the extra row on target with per-{@code (cf, ts)}
   * {@code DeleteFamilyVersion}; bump {@code rowsExtra} when at least one live cell was
   * tombstoned, else {@code rowsCannotRepair} (row was already all tombstones).</li>
   * </ul>
   */
  private void repairDiffRows(ResultScanner sourceScanner, ResultScanner targetScanner,
    Table targetHTable, DriftCounters drift, Progressable progress) throws IOException {
    List<Put> pendingPuts = new ArrayList<>();
    List<Delete> pendingDeletes = new ArrayList<>();
    Result sourceResult = sourceScanner.next();
    Result targetResult = targetScanner.next();

    while (sourceResult != null || targetResult != null) {
      int cmp = compareRowKeys(sourceResult, targetResult);
      if (cmp == 0) {
        RowDriftInfo rowDriftInfo = generateMutationForDiffCells(sourceResult, targetResult,
          targetHTable, pendingPuts, pendingDeletes);
        drift.addCellDrift(rowDriftInfo.cells);
        if (rowDriftInfo.rowCannotRepair) {
          drift.rowsCannotRepair++;
        }
        if (rowDriftInfo != RowDriftInfo.NONE) {
          LOGGER.warn(
            "Row mismatch on table {} row={}: cell drift missing={}, extra={}, different={}, "
              + "rowCannotRepair={}",
            tableName, Bytes.toStringBinary(sourceResult.getRow()), rowDriftInfo.cells.missing,
            rowDriftInfo.cells.extra, rowDriftInfo.cells.different, rowDriftInfo.rowCannotRepair);
        }
        sourceResult = sourceScanner.next();
        targetResult = targetScanner.next();
      } else if (cmp < 0) {
        byte[] missingRowKey = sourceResult.getRow();
        RowMirrorStatus outcome =
          mirrorWholeRow(sourceResult, targetHTable, pendingPuts, pendingDeletes);
        if (outcome != RowMirrorStatus.FULLY_SHADOWED) {
          drift.rowsMissingOnTarget++;
        }
        if (outcome != RowMirrorStatus.FULLY_MIRRORED) {
          drift.rowsCannotRepair++;
        }
        LOGGER.warn("Row missing on target for table {} row={}: mirrorOutcome={}", tableName,
          Bytes.toStringBinary(missingRowKey), outcome);
        sourceResult = sourceScanner.next();
      } else {
        byte[] extraRowKey = targetResult.getRow();
        int liveCellsTombstoned =
          tombstoneWholeRow(targetResult, pendingPuts, pendingDeletes);
        if (liveCellsTombstoned == 0) {
          drift.rowsCannotRepair++;
        } else {
          drift.rowsExtraOnTarget++;
        }
        LOGGER.warn("Row extra on target for table {} row={}: liveCellsTombstoned={}", tableName,
          Bytes.toStringBinary(extraRowKey), liveCellsTombstoned);
        targetResult = targetScanner.next();
      }

      if (pendingPuts.size() + pendingDeletes.size() >= repairBatchSize) {
        flushRepairMutations(targetHTable, pendingPuts, pendingDeletes);
      }
      if (progress != null) {
        progress.progress();
      }
    }
    flushRepairMutations(targetHTable, pendingPuts, pendingDeletes);
  }

  /**
   * Compares the row keys of two scanner results; treats a null result as past-end so a
   * {@code null/non-null} pair sorts the non-null side first.
   */
  private static int compareRowKeys(Result sourceResult, Result targetResult) {
    if (sourceResult == null) {
      return 1;
    }
    if (targetResult == null) {
      return -1;
    }
    return Bytes.compareTo(sourceResult.getRow(), targetResult.getRow());
  }

  /**
   * Whole-row content equality check used by dry-run row-level diffing. Delegates to
   * {@link Result#compareResults(Result, Result, boolean)} which throws on any cell-level mismatch
   * (family, qualifier, timestamp, type, value); we map the throw to {@code false} so the cmp==0
   * path can flag the row without producing repair mutations.
   */
  private boolean rowsEqual(Result src, Result tgt) {
    try {
      Result.compareResults(src, tgt, false);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Mirrors every source cell of a row that is missing on target. Iterates cell-by-cell (not one
   * whole-row {@link Put}) so each cell can be shadow-checked against target's tombstones via
   * {@link TargetRowRecord} — a Put shadowed by an existing target tombstone would land on disk
   * invisible to reads, silently faking a {@code REPAIRED} chunk. Suppressed mirrors flip the row
   * to {@code PARTIALLY_MIRRORED} / {@code FULLY_SHADOWED} so the chunk resolves as
   * {@code UNREPAIRABLE}. Per-cell iteration also lets {@code --raw-scan} tombstone cells route
   * through {@link Delete#add(Cell)}, which {@link Put#add(Cell)} would reject.
   */
  private RowMirrorStatus mirrorWholeRow(Result sourceResult, Table targetHTable,
    List<Put> pendingPuts, List<Delete> pendingDeletes) throws IOException {
    RowRepairBuffer rowRepairBuffer = new RowRepairBuffer(sourceResult.getRow());
    int mirrored = 0;
    for (Cell cell : sourceResult.rawCells()) {
      if (mirrorSourceCellUnlessShadowed(cell, targetHTable, rowRepairBuffer)) {
        mirrored++;
      }
    }
    rowRepairBuffer.flush(pendingPuts, pendingDeletes);
    if (mirrored == 0) {
      return RowMirrorStatus.FULLY_SHADOWED;
    }
    return rowRepairBuffer.anyCellUnrepairable
      ? RowMirrorStatus.PARTIALLY_MIRRORED
      : RowMirrorStatus.FULLY_MIRRORED;
  }

  /**
   * Tombstones every live cell of a row that is extra on target by emitting one
   * {@link Delete#addFamilyVersion} per distinct {@code (family, ts)} observed. Skips cells that
   * are themselves already tombstones.
   * <p>
   * {@code DeleteFamilyVersion} shadows every Put at {@code (cf, *, ts == T)} exactly, so it never
   * reaches cells outside {@code [fromTime, toTime)} — the scan already time-bounded them.
   * @return the number of live cells that contributed a tombstone marker. {@code 0} means the row
   *         was already entirely tombstones; the caller records this as {@code ROWS_CANNOT_REPAIR}.
   */
  private int tombstoneWholeRow(Result targetResult, List<Put> pendingPuts,
    List<Delete> pendingDeletes) {
    RowRepairBuffer rowRepairBuffer = new RowRepairBuffer(targetResult.getRow());
    Set<CfTsKey> tombstoned = new HashSet<>();
    int liveCellsTombstoned = 0;
    for (Cell cell : targetResult.rawCells()) {
      if (CellUtil.isDelete(cell)) {
        continue;
      }
      CfTsKey key = CfTsKey.of(cell);
      if (tombstoned.add(key)) {
        rowRepairBuffer.delete().addFamilyVersion(key.family, key.ts);
      }
      liveCellsTombstoned++;
    }
    rowRepairBuffer.flush(pendingPuts, pendingDeletes);
    return liveCellsTombstoned;
  }

  /**
   * Diffs cells of two rows present on both clusters by grouping Put cells on both sides by
   * {@code (family, ts)}, then classifying each group:
   * <ul>
   * <li><b>target-only {@code (cf, ts)}</b> → one {@link Delete#addFamilyVersion}; wipes the whole
   * slice in one marker regardless of qualifier count. Bumps {@code cellExtra} by group size.</li>
   * <li><b>source-only {@code (cf, ts)}</b> → mirror each source cell (shadow-checked). Bumps
   * {@code cellMissing} per mirrored cell.</li>
   * <li><b>same {@code (cf, ts)}, different content</b> → qualifier-level diff within the group:
   *   <ul>
   *   <li>source-only qualifier → mirror (shadow-checked); {@code cellMissing++}.</li>
   *   <li>same qualifier, different value → mirror source cell; {@code cellDifferent++}. HBase
   *   uses the later-written cell at same {@code (cf, q, ts)}, so a plain Put wins with no
   *   companion delete needed.</li>
   *   <li>target-only qualifier → point-{@link Delete#addColumn} at exact ts; {@code cellExtra++}.
   *   Cannot emit {@code DeleteFamilyVersion(cf, ts)} here — it would shadow our own same-ts
   *   mirrored Puts in the same batch.</li>
   *   </ul>
   * </li>
   * </ul>
   * Source tombstones bypass the group diff and mirror as-is (subtype preserved). Mirrors
   * suppressed by shadowing do NOT bump the cell counter (nothing was written); the row-level
   * signal flows through {@link RowDriftInfo#rowCannotRepair}.
   * <p>
   * Target's raw+all-versions scan surfaces hidden (max-versions-filtered) Puts directly in
   * {@code targetResult.rawCells()}, so their timestamps appear as their own target-only
   * {@code (cf, ts)} groups and get {@code DeleteFamilyVersion}'d without any separate hidden-
   * version-discovery RPC.
   */
  private RowDriftInfo generateMutationForDiffCells(Result sourceResult, Result targetResult,
    Table targetHTable, List<Put> pendingPuts, List<Delete> pendingDeletes) throws IOException {
    Cell[] sourceCells = sourceResult.rawCells();
    Cell[] targetCells = targetResult.rawCells();
    RowRepairBuffer rowRepairBuffer = new RowRepairBuffer(sourceResult.getRow());

    Map<CfTsKey, Map<ColumnKey, Cell>> srcPutsByCfTs = new HashMap<>();
    Map<CfTsKey, Map<ColumnKey, Cell>> tgtPutsByCfTs = new HashMap<>();
    List<Cell> srcTombstones = new ArrayList<>();

    for (Cell c : sourceCells) {
      if (CellUtil.isDelete(c)) {
        srcTombstones.add(c);
      } else {
        srcPutsByCfTs.computeIfAbsent(CfTsKey.of(c), k -> new HashMap<>()).put(ColumnKey.of(c), c);
      }
    }
    for (Cell c : targetCells) {
      if (!CellUtil.isDelete(c)) {
        tgtPutsByCfTs.computeIfAbsent(CfTsKey.of(c), k -> new HashMap<>()).put(ColumnKey.of(c), c);
      }
    }

    // Mirror source tombstones directly; subtype preserved via Delete.add(Cell). Source tombstones
    // can't be shadowed by target tombstones (only source Puts can), so no shadow-check here.
    for (Cell tomb : srcTombstones) {
      rowRepairBuffer.delete().add(tomb);
    }

    int cellMissing = 0;
    int cellExtra = 0;
    int cellDifferent = 0;

    Set<CfTsKey> allKeys = new HashSet<>(srcPutsByCfTs.keySet());
    allKeys.addAll(tgtPutsByCfTs.keySet());

    for (CfTsKey key : allKeys) {
      Map<ColumnKey, Cell> srcGroup = srcPutsByCfTs.getOrDefault(key, Collections.emptyMap());
      Map<ColumnKey, Cell> tgtGroup = tgtPutsByCfTs.getOrDefault(key, Collections.emptyMap());

      if (srcGroup.isEmpty()) {
        // Target-only (cf, ts): one marker wipes the whole slice.
        rowRepairBuffer.delete().addFamilyVersion(key.family, key.ts);
        cellExtra += tgtGroup.size();
      } else if (tgtGroup.isEmpty()) {
        // Source-only (cf, ts): mirror each source cell (shadow-checked).
        for (Cell c : srcGroup.values()) {
          if (mirrorSourceCellUnlessShadowed(c, targetHTable, rowRepairBuffer)) {
            cellMissing++;
          }
        }
      } else {
        // Same (cf, ts) different content: qualifier-level diff. Cannot use DeleteFamilyVersion
        // here — it would shadow our own same-ts Puts in the same batch.
        for (Map.Entry<ColumnKey, Cell> srcEntry : srcGroup.entrySet()) {
          Cell tgtCell = tgtGroup.get(srcEntry.getKey());
          Cell srcCell = srcEntry.getValue();
          if (tgtCell == null) {
            if (mirrorSourceCellUnlessShadowed(srcCell, targetHTable, rowRepairBuffer)) {
              cellMissing++;
            }
          } else if (!CellUtil.matchingValue(srcCell, tgtCell)) {
            if (mirrorSourceCellUnlessShadowed(srcCell, targetHTable, rowRepairBuffer)) {
              cellDifferent++;
            }
          }
        }
        for (Map.Entry<ColumnKey, Cell> tgtEntry : tgtGroup.entrySet()) {
          if (!srcGroup.containsKey(tgtEntry.getKey())) {
            Cell c = tgtEntry.getValue();
            rowRepairBuffer.delete().addColumn(CellUtil.cloneFamily(c),
              CellUtil.cloneQualifier(c), c.getTimestamp());
            cellExtra++;
          }
        }
      }
    }

    if (
      cellMissing == 0 && cellExtra == 0 && cellDifferent == 0 && srcTombstones.isEmpty()
        && !rowRepairBuffer.anyCellUnrepairable
    ) {
      return RowDriftInfo.NONE;
    }
    rowRepairBuffer.flush(pendingPuts, pendingDeletes);
    return new RowDriftInfo(new CellDriftCounts(cellMissing, cellExtra, cellDifferent),
      rowRepairBuffer.anyCellUnrepairable);
  }

  /**
   * Routes a source cell to the right mutation kind. Tombstone cells go through
   * {@link Delete#add(Cell)} (preserves the exact tombstone subtype); under {@code --raw-scan} this
   * matters because {@link Put#add(Cell)} rejects non-Put cells.
   */
  private void mirrorSourceCell(Cell cell, RowRepairBuffer rowRepairBuffer) throws IOException {
    if (CellUtil.isDelete(cell)) {
      rowRepairBuffer.delete().add(cell);
    } else {
      rowRepairBuffer.put().add(cell);
    }
  }

  /**
   * Mirrors a source cell onto target unless an existing target tombstone would shadow it. Shadow
   * detection runs only if source has Put cells; tombstoned source cells always mirror.
   * @return {@code true} if mirrored, {@code false} if suppressed (caller marks the row
   *         unrepairable).
   */
  private boolean mirrorSourceCellUnlessShadowed(Cell cell, Table targetHTable,
    RowRepairBuffer rowRepairBuffer) throws IOException {
    // Source Puts can be shadowed by an existing target tombstone, the Put lands on
    // disk but stays invisible to reads, so writing it is wasted work and the row stays
    // diverged. e.g. src Put(name, T=200) vs tgt DeleteColumn(name, T=300) covering
    // ts<=300. Skip the write and flag the row unrepairable; operator must major-compact
    // target to reap the shadow. Source tombstones can't be shadowed, hence skip the check.
    if (
      !CellUtil.isDelete(cell) && rowRepairBuffer.targetRowRecord(targetHTable).wouldShadow(cell)
    ) {
      rowRepairBuffer.anyCellUnrepairable = true;
      return false;
    }
    mirrorSourceCell(cell, rowRepairBuffer);
    return true;
  }

  /**
   * Builds a row-level HBase scan for repair.
   * <p>
   * {@code forceRawAllVersions=true} (used for the target repair scan) sets {@code raw=true} and
   * {@code readAllVersions()} unconditionally so tombstone cells and hidden (max-versions-filtered)
   * Puts surface directly in the merge walk — the ts-grouped diff turns hidden target versions
   * into their own target-only {@code (cf, ts)} groups that get {@code DeleteFamilyVersion}'d
   * without a separate hidden-version-discovery RPC.
   * <p>
   * {@code forceRawAllVersions=false} (used for the source repair scan) honors the user's
   * {@code --raw-scan} and {@code --read-all-versions} flags so the mirrored source view matches
   * the cells the verifier hashed.
   * <p>
   * Adds bulk caching plus Phoenix TTL / {@code IS_STRICT_TTL} attributes so the cells visited
   * here are the same cells the verifier hashed.
   */
  private Scan createRepairScan(byte[] startKey, byte[] endKey, boolean isStartKeyInclusive,
    boolean isEndKeyInclusive, boolean forceRawAllVersions, PhoenixConnection phoenixConn)
    throws IOException, SQLException {
    Scan scan = new Scan();
    scan.withStartRow(startKey, isStartKeyInclusive);
    scan.withStopRow(endKey, isEndKeyInclusive);
    scan.setRaw(forceRawAllVersions || isRawScan);
    if (forceRawAllVersions || isReadAllVersions) {
      scan.readAllVersions();
    }
    scan.setCacheBlocks(false);
    scan.setTimeRange(fromTime, toTime);
    scan.setCaching(1000);
    ScanUtil.setScanAttributesForPhoenixTTL(scan, pTable, phoenixConn);
    scan.setAttribute(BaseScannerRegionObserverConstants.IS_STRICT_TTL, TRUE_BYTES);
    return scan;
  }

  /**
   * Flushes the accumulated Put and Delete batches to target as a single mixed RPC via
   * {@link Table#batch}. The mixed batch (rather than separate {@code put()} + {@code delete()}
   * calls) closes the inter-RPC window where a JVM/regionserver crash between the two could leave
   * target with Puts applied but matching Deletes missing.
   * <p>
   * {@link Table#batch} does NOT throw for partial failures — per-mutation failures (e.g.
   * {@code NotServingRegionException} from a region split mid-batch, {@code WrongRegionException}
   * from a merge) land in the {@code results} array as {@link Throwable} entries. We surface the
   * first such failure as {@link IOException} so the caller treats this chunk as
   * {@code REPAIR_FAILED} rather than silently marking it {@code REPAIRED}; on re-run the resume
   * filter excludes {@code REPAIR_FAILED} and the chunk re-enters as an unprocessed gap.
   */
  private void flushRepairMutations(Table targetHTable, List<Put> puts, List<Delete> deletes)
    throws IOException {
    if (puts.isEmpty() && deletes.isEmpty()) {
      return;
    }
    List<Row> mutations = new ArrayList<>(puts.size() + deletes.size());
    mutations.addAll(puts);
    mutations.addAll(deletes);
    Object[] results = new Object[mutations.size()];
    try {
      targetHTable.batch(mutations, results);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while flushing repair mutations", e);
    }
    int failureCount = 0;
    int firstFailureIdx = -1;
    for (int i = 0; i < results.length; i++) {
      if (results[i] instanceof Throwable) {
        failureCount++;
        if (firstFailureIdx < 0) {
          firstFailureIdx = i;
        }
      }
    }
    if (failureCount > 0) {
      Throwable firstFailure = (Throwable) results[firstFailureIdx];
      Row failedRow = mutations.get(firstFailureIdx);
      throw new IOException(
        String.format("Repair batch had %d/%d mutation failure(s); first failure on row %s: %s",
          failureCount, results.length, Bytes.toStringBinary(failedRow.getRow()),
          firstFailure.getMessage()),
        firstFailure);
    }
    puts.clear();
    deletes.clear();
  }

  /**
   * Inputs to a chunk repair attempt. Source range is the chunk boundary; target range may be wider
   * so the repair scan sees the same cells (including extra-on-target rows between consecutive
   * source chunks) that the verifier hashed.
   * <p>
   * {@link #verifySourceRows} / {@link #verifyTargetRows} are the row counts the verifier recorded;
   * threaded into the COUNTERS column on the resulting checkpoint row. {@link #verifyStartTime} is
   * the timestamp captured when verification began for this chunk; reused as EXECUTION_START_TIME
   * on the REPAIRED/UNREPAIRABLE/REPAIR_FAILED checkpoint row so the row spans the full
   * verify+repair lifecycle that overwrites the MISMATCHED row.
   */
  public static final class ChunkRepairRequest {
    public final byte[] sourceStart;
    public final byte[] sourceEnd;
    public final byte[] targetStart;
    public final byte[] targetEnd;
    public final boolean targetStartInclusive;
    public final boolean targetEndInclusive;
    public final long verifySourceRows;
    public final long verifyTargetRows;
    public final Timestamp verifyStartTime;
    public final boolean dryRun;

    public ChunkRepairRequest(byte[] sourceStart, byte[] sourceEnd, byte[] targetStart,
      byte[] targetEnd, boolean targetStartInclusive, boolean targetEndInclusive,
      long verifySourceRows, long verifyTargetRows, Timestamp verifyStartTime, boolean dryRun) {
      this.sourceStart = sourceStart;
      this.sourceEnd = sourceEnd;
      this.targetStart = targetStart;
      this.targetEnd = targetEnd;
      this.targetStartInclusive = targetStartInclusive;
      this.targetEndInclusive = targetEndInclusive;
      this.verifySourceRows = verifySourceRows;
      this.verifyTargetRows = verifyTargetRows;
      this.verifyStartTime = verifyStartTime;
      this.dryRun = dryRun;
    }
  }

  /**
   * Outcome of a chunk repair attempt. Carries the terminal status, accumulated drift counters,
   * end-of-attempt timestamp, and the failure exception when status is
   * {@link Status#REPAIR_FAILED}. Status precedence (most-severe wins):
   * {@link Status#REPAIR_FAILED} &gt; {@link Status#UNREPAIRABLE} &gt; {@link Status#REPAIRED}.
   */
  public static final class ChunkRepairResult {

    public enum Status {
      REPAIRED,
      UNREPAIRABLE,
      REPAIR_FAILED
    }

    public final Status status;
    public final DriftCounters drift;
    public final Timestamp endTime;
    public final IOException failure;

    private ChunkRepairResult(Status status, DriftCounters drift, Timestamp endTime,
      IOException failure) {
      this.status = status;
      this.drift = drift;
      this.endTime = endTime;
      this.failure = failure;
    }

    static ChunkRepairResult completed(DriftCounters drift) {
      Status status = drift.rowsCannotRepair > 0 ? Status.UNREPAIRABLE : Status.REPAIRED;
      return new ChunkRepairResult(status, drift, new Timestamp(System.currentTimeMillis()), null);
    }

    static ChunkRepairResult failed(DriftCounters drift, IOException failure) {
      return new ChunkRepairResult(Status.REPAIR_FAILED, drift,
        new Timestamp(System.currentTimeMillis()), failure);
    }
  }

  /**
   * Per-chunk aggregate of six drift counters — three row-level ({@code rowsMissingOnTarget},
   * {@code rowsExtraOnTarget}, {@code rowsCannotRepair}) and three cell-level
   * ({@code cellsMissing/Extra/DifferentOnTarget}). Pure accumulator; the caller maps fields onto
   * MapReduce job counters and the checkpoint COUNTERS string.
   */
  public static final class DriftCounters {
    public long rowsMissingOnTarget;
    public long rowsExtraOnTarget;
    public long rowsDifferentOnTarget;
    public long rowsCannotRepair;
    public long cellsMissingOnTarget;
    public long cellsExtraOnTarget;
    public long cellsDifferentOnTarget;

    void addCellDrift(CellDriftCounts cellDrift) {
      cellsMissingOnTarget += cellDrift.missing;
      cellsExtraOnTarget += cellDrift.extra;
      cellsDifferentOnTarget += cellDrift.different;
    }

    /** Compact end-of-chunk log line summarizing all drift signals. */
    public String toLogString() {
      return String.format(
        "rowsMissingOnTarget=%d, rowsExtraOnTarget=%d, rowsDifferentOnTarget=%d, "
          + "rowsCannotRepair=%d, cellsMissingOnTarget=%d, cellsExtraOnTarget=%d, "
          + "cellsDifferentOnTarget=%d",
        rowsMissingOnTarget, rowsExtraOnTarget, rowsDifferentOnTarget, rowsCannotRepair,
        cellsMissingOnTarget, cellsExtraOnTarget, cellsDifferentOnTarget);
    }
  }

  /**
   * Per-row snapshot of target's tombstones used by {@link #wouldShadow} for shadow detection
   * before mirroring a source Put.
   * <p>
   * Hidden-Put discovery is now handled inline by the {@code (cf, ts)}-grouped diff in
   * {@link #generateMutationForDiffCells} using target's raw+all-versions scan cells directly, so
   * this record no longer indexes target Puts — only tombstones.
   * <p>
   * HBase has four tombstone subtypes; each is recorded into its own map because shadow scope
   * differs:
   *
   * <pre>
   *   Delete               shadows Put at (cf, q, ts == T) exactly
   *   DeleteColumn         shadows Puts at (cf, q, ts &lt;= T)
   *   DeleteFamily         shadows Puts at (cf, *, ts &lt;= T)
   *   DeleteFamilyVersion  shadows Puts at (cf, *, ts == T)
   * </pre>
   */
  static final class TargetRowRecord {
    private final Map<ColumnKey, Set<Long>> deletePointTs = new HashMap<>();
    private final Map<ColumnKey, Long> deleteColumnUpperBound = new HashMap<>();
    private final Map<ByteBuffer, Long> deleteFamilyUpperBound = new HashMap<>();
    private final Map<ByteBuffer, Set<Long>> deleteFamilyVersionTs = new HashMap<>();

    /**
     * Builds a {@link TargetRowRecord} from a single-row HBase scan.
     * <p>
     * <b>raw=true + all-versions</b> are forced regardless of user flags so tombstones (the only
     * thing this record now captures) are surfaced.
     * <p>
     * <b>Time range {@code [fromTime, MAX_VALUE]}</b>:
     * <ul>
     * <li>Lower bound = {@code fromTime}: cells below the verify window can't affect repair inside
     * the window.</li>
     * <li>Upper bound = {@code MAX_VALUE} (NOT {@code toTime}): a tombstone at {@code ts >= toTime}
     * can still shadow a Put we mirror at {@code ts} in window during application reads, so we must
     * see it. e.g. window {@code [0, 600)}, tgt has DeleteColumn@900, src wants Put@500 — without
     * the wide upper bound we'd miss the 900 tombstone and write a doomed mirror.</li>
     * </ul>
     */
    static TargetRowRecord load(byte[] rowKey, Table targetHTable, long fromTime)
      throws IOException {
      Scan scan = new Scan();
      scan.withStartRow(rowKey, true);
      scan.withStopRow(rowKey, true);
      scan.setRaw(true);
      scan.readAllVersions();
      scan.setCacheBlocks(false);
      scan.setTimeRange(fromTime, Long.MAX_VALUE);
      scan.setCaching(1);
      scan.setLimit(1);
      TargetRowRecord rowRecord = new TargetRowRecord();
      try (ResultScanner scanner = targetHTable.getScanner(scan)) {
        Result raw = scanner.next();
        if (raw != null) {
          for (Cell cell : raw.rawCells()) {
            if (CellUtil.isDelete(cell)) {
              rowRecord.recordTombstone(cell);
            }
          }
        }
      }
      return rowRecord;
    }

    /**
     * Records one tombstone into its per-subtype map for {@link #wouldShadow} to query.
     * {@code <=ts} delete subtypes ({@code DeleteColumn}, {@code DeleteFamily}) collapse to the max
     * ts; exact-ts subtypes ({@code Delete}, {@code DeleteFamilyVersion}) accumulate into a set.
     */
    private void recordTombstone(Cell tombstone) {
      long ts = tombstone.getTimestamp();
      ByteBuffer family = ByteBuffer.wrap(CellUtil.cloneFamily(tombstone));
      switch (tombstone.getType()) {
        case Delete:
          deletePointTs.computeIfAbsent(ColumnKey.of(tombstone), k -> new HashSet<>()).add(ts);
          break;
        case DeleteColumn:
          deleteColumnUpperBound.merge(ColumnKey.of(tombstone), ts, Math::max);
          break;
        case DeleteFamily:
          deleteFamilyUpperBound.merge(family, ts, Math::max);
          break;
        case DeleteFamilyVersion:
          deleteFamilyVersionTs.computeIfAbsent(family, k -> new HashSet<>()).add(ts);
          break;
        default:
          // Unreachable: caller filters via CellUtil.isDelete.
      }
    }

    /** Returns true if any tombstone recorded here would shadow a Put at the cell's coords. */
    boolean wouldShadow(Cell sourcePut) {
      long ts = sourcePut.getTimestamp();
      ByteBuffer family = ByteBuffer.wrap(CellUtil.cloneFamily(sourcePut));
      ColumnKey column = ColumnKey.of(sourcePut);

      // Delete: shadows Put at exactly (cf, q, ts == T).
      Set<Long> pointTs = deletePointTs.get(column);
      if (pointTs != null && pointTs.contains(ts)) {
        return true;
      }
      // DeleteColumn: shadows every Put at (cf, q) with ts <= T.
      Long deleteColTs = deleteColumnUpperBound.get(column);
      if (deleteColTs != null && ts <= deleteColTs) {
        return true;
      }
      // DeleteFamily: shadows every Put across all qualifiers in cf with ts <= T.
      Long deleteFamTs = deleteFamilyUpperBound.get(family);
      if (deleteFamTs != null && ts <= deleteFamTs) {
        return true;
      }
      // DeleteFamilyVersion: shadows Puts across all qualifiers in cf at exactly ts == T.
      Set<Long> dfvTs = deleteFamilyVersionTs.get(family);
      return dfvTs != null && dfvTs.contains(ts);
    }
  }

  /** Composite (family, qualifier) key with byte-array equality semantics. */
  static final class ColumnKey {
    private final byte[] family;
    private final byte[] qualifier;

    ColumnKey(byte[] family, byte[] qualifier) {
      this.family = family;
      this.qualifier = qualifier;
    }

    static ColumnKey of(Cell cell) {
      return new ColumnKey(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell));
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof ColumnKey)) {
        return false;
      }
      ColumnKey other = (ColumnKey) o;
      return Bytes.equals(family, other.family) && Bytes.equals(qualifier, other.qualifier);
    }

    @Override
    public int hashCode() {
      return Bytes.hashCode(family) * 31 + Bytes.hashCode(qualifier);
    }
  }

  /**
   * Composite (family, timestamp) key used to bucket Put cells for the {@code (cf, ts)}-grouped
   * diff in {@link #generateMutationForDiffCells}. Byte-array equality on family.
   */
  static final class CfTsKey {
    final byte[] family;
    final long ts;

    CfTsKey(byte[] family, long ts) {
      this.family = family;
      this.ts = ts;
    }

    static CfTsKey of(Cell cell) {
      return new CfTsKey(CellUtil.cloneFamily(cell), cell.getTimestamp());
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof CfTsKey)) {
        return false;
      }
      CfTsKey other = (CfTsKey) o;
      return ts == other.ts && Bytes.equals(family, other.family);
    }

    @Override
    public int hashCode() {
      return Bytes.hashCode(family) * 31 + Long.hashCode(ts);
    }
  }

  /**
   * Per-row scratch buffer: lazily-built {@link Put}/{@link Delete} mutations, lazily-loaded
   * {@link TargetRowRecord}, and an unrepairable-drift flag the caller reads after the merge.
   */
  final class RowRepairBuffer {
    private final byte[] rowKey;
    Put put;
    Delete delete;
    TargetRowRecord targetRowRecord;
    boolean anyCellUnrepairable;

    RowRepairBuffer(byte[] rowKey) {
      this.rowKey = rowKey;
    }

    Put put() {
      if (put == null) {
        put = new Put(rowKey);
      }
      return put;
    }

    Delete delete() {
      if (delete == null) {
        delete = new Delete(rowKey);
      }
      return delete;
    }

    /**
     * Returns the cached {@link TargetRowRecord} for this row, loading on first call via
     * {@link TargetRowRecord#load} (one raw all-versions scan, time range
     * {@code [fromTime, MAX_VALUE]}). Cache scope is the buffer's lifetime — i.e. the current row —
     * so repeated cell-level shadow checks within the row pay one round-trip total.
     * <p>
     * Consumed by {@link #mirrorSourceCellUnlessShadowed}, which asks
     * {@link TargetRowRecord#wouldShadow} before mirroring a source Put, to skip writes that
     * target's existing tombstones would render invisible.
     *
     * <pre>
     *   target row state: DeleteColumn(NAME)@T=900   (covers ts &lt;= 900)
     *   source row state: Put(NAME, "alice")@T=500
     *   wouldShadow(srcPut@500) → true
     *   ⇒ skip mirror, mark row unrepairable; operator must major-compact target
     * </pre>
     */
    TargetRowRecord targetRowRecord(Table targetHTable) throws IOException {
      if (targetRowRecord == null) {
        targetRowRecord = TargetRowRecord.load(rowKey, targetHTable, fromTime);
      }
      return targetRowRecord;
    }

    void flush(List<Put> pendingPuts, List<Delete> pendingDeletes) {
      if (put != null) {
        pendingPuts.add(put);
      }
      if (delete != null) {
        pendingDeletes.add(delete);
      }
    }
  }

  /**
   * Cell-level drift counts produced by per-row diff. Three counters partition the cell differences
   * into disjoint buckets — source-only, target-only-live, same-coord-diff-value.
   */
  static final class CellDriftCounts {
    static final CellDriftCounts NONE = new CellDriftCounts(0, 0, 0);

    final int missing;
    final int extra;
    final int different;

    CellDriftCounts(int missing, int extra, int different) {
      this.missing = missing;
      this.extra = extra;
      this.different = different;
    }
  }

  /** Per-row drift summary: cell-level drift counts plus a row-unrepairable flag. */
  static final class RowDriftInfo {
    static final RowDriftInfo NONE = new RowDriftInfo(CellDriftCounts.NONE, false);

    final CellDriftCounts cells;
    final boolean rowCannotRepair;

    RowDriftInfo(CellDriftCounts cells, boolean rowCannotRepair) {
      this.cells = cells;
      this.rowCannotRepair = rowCannotRepair;
    }
  }

  /** Terminal classification of a per-row mirror attempt onto target. */
  enum RowMirrorStatus {
    /** All source cells mirrored in row. */
    FULLY_MIRRORED,
    /** Some mirrored, some suppressed by target tombstones. */
    PARTIALLY_MIRRORED,
    /** Every source cell suppressed by target tombstones. */
    FULLY_SHADOWED
  }
}
