package org.greenplum.pxf.plugins.clickhouse.writercallable;

import org.apache.commons.collections.MapUtils;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.clickhouse.ClickHouseBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Greenplum Segment Id Data Writer Clickhouse Shard
 */
public class SegmentIdSortUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentIdSortUtils.class);

    private int poolSize;
    private int batchSize;
    private int jdbcShardSize;
    private ClickHouseBasePlugin plugin;
    private String query;
    private SegmentIdDataWriter segmentIdDataWriter;
    private Map<Integer, SegmentIdDataWriter> writerCallableMap;

    private ExecutorService executorServiceWrite = null;
    private List<Future<SQLException>> poolTasks = null;


    /**
     * Create a new instance of the factory.
     *
     */
    public SegmentIdSortUtils(ClickHouseBasePlugin plugin, String query, int batchSize, int poolSize) throws SQLException {
        this.plugin = plugin;
        this.query = query;
        this.batchSize = batchSize;
        this.poolSize = poolSize;

        // 默认开启按segmentId写入分片
        jdbcShardSize = plugin.jdbcUrlShardReplicas.size();

        if (jdbcShardSize > 1 && plugin.shardWriterEnabled) { // shard 数大于1 且需要按照segmentId来写入shard
            writerCallableMap = new ConcurrentHashMap<>(plugin.getRequestContext().getTotalSegments());
        } else {
            segmentIdDataWriter = new SegmentIdDataWriter(plugin, query, batchSize);
        }

        if (plugin.shardWriterEnabled && plugin.segmentColumnIndex == null) {
            throw new IllegalArgumentException("not exists column: " + plugin.segmentColumnName);
        }

        if (this.poolSize > 1 || jdbcShardSize > 1) {
            int nThreads = poolSize * jdbcShardSize;
            executorServiceWrite = Executors.newFixedThreadPool(nThreads > 5 ? 5 : nThreads); // 最多5个同时写入
            poolTasks = new LinkedList<>();
        }
    }


    public void supply(OneRow row) throws Exception {
        if (jdbcShardSize > 1 && plugin.shardWriterEnabled) {
            Integer segmentId = (Integer) row.getKey();
            SegmentIdDataWriter segmentIdDataWriter = null;
            if (writerCallableMap.containsKey(segmentId)) {
                segmentIdDataWriter = writerCallableMap.get(segmentId);
            } else {
                segmentIdDataWriter = new SegmentIdDataWriter(plugin, query, segmentId, batchSize);
                writerCallableMap.put(segmentId, segmentIdDataWriter);
            }

            segmentIdDataWriter.supply(row);
            if (segmentIdDataWriter.isCallRequired()) {
                // Pooling is used. Create new writerCallable
                poolTasks.add(executorServiceWrite.submit(segmentIdDataWriter));
                writerCallableMap.remove(segmentId);
            }
        } else {
            segmentIdDataWriter.supply(row);
            if (segmentIdDataWriter.isCallRequired()) {
                if (poolSize > 1) {
                    // Pooling is used. Create new writerCallable
                    poolTasks.add(executorServiceWrite.submit(segmentIdDataWriter));
                } else {
                    // Pooling is not used, call directly and process potential error
                    SQLException e = segmentIdDataWriter.call();
                    if (e != null) {
                        throw e;
                    }
                }
                segmentIdDataWriter = new SegmentIdDataWriter(plugin, query, batchSize);
            }
        }
    }


    public void close() throws Exception {
        // Process thread pool
        Exception firstException = null;
        if (this.poolSize > 1 || jdbcShardSize > 1) {
            for (Future<SQLException> task : poolTasks) {
                // We need this construction to ensure that we try to close all connections opened by pool threads
                try {
                    SQLException currentSqlException = task.get();
                    if (currentSqlException != null) {
                        if (firstException == null) {
                            firstException = currentSqlException;
                        }
                        LOG.error(
                                "A SQLException in a pool thread occurred: " + currentSqlException.getClass() + " " + currentSqlException.getMessage()
                        );
                    }
                } catch (Exception e) {
                    // This exception must have been caused by some thread execution error. However, there may be other exception (maybe of class SQLException) that happened in one of threads that were not examined yet. That is why we do not modify firstException
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "A runtime exception in a thread pool occurred: " + e.getClass() + " " + e.getMessage()
                        );
                    }
                }
            }
            try {
                executorServiceWrite.shutdown();
                executorServiceWrite.shutdownNow();
            } catch (Exception e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("executorServiceWrite.shutdown() or .shutdownNow() threw an exception: " + e.getClass() + " " + e.getMessage());
                }
            }
        }

        // Send data that is left
        if (segmentIdDataWriter != null) {
            try {
                firstException = segmentIdDataWriter.call();
            } catch (Exception e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("A runtime exception in a writerCallable occurred: " + e.getClass() + " " + e.getMessage());
                }
            }
        }
        if (MapUtils.isNotEmpty(writerCallableMap)) {
            for (Map.Entry<Integer, SegmentIdDataWriter> writerCallableEntry : writerCallableMap.entrySet()) {
                try {
                    firstException = writerCallableEntry.getValue().call();
                } catch (Exception e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("A runtime exception in a writerCallableMap occurred: " + e.getClass() + " " + e.getMessage());
                    }
                }
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }
}
