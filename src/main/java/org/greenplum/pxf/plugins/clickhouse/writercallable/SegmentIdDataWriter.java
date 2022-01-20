package org.greenplum.pxf.plugins.clickhouse.writercallable;

import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.plugins.clickhouse.ClickHouseBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHousePreparedStatement;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

class SegmentIdDataWriter implements WriterCallable {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentIdDataWriter.class);

    @Override
    public void supply(OneRow row) throws IllegalStateException {
        if ((batchSize > 0) && (rows >= batchSize)) {
            throw new IllegalStateException("Trying to supply() a OneRow object to a full WriterCallable");
        }
        try {
            statement.unwrap(ClickHousePreparedStatement.class).addRow((StringBuilder) row.getData());
        } catch (Exception e) {
            LOG.error("add row error", e);
            throw new IllegalArgumentException("Trying to supply() a null OneRow object", e);
        }
        rows++;
    }

    @Override
    public boolean isCallRequired() {
        return (batchSize > 0) && (rows >= batchSize);
    }

    @Override
    public SQLException call() throws SQLException {
        if (rows == 0) {
            ClickHouseBasePlugin.closeStatementAndConnection(statement);
            return null;
        }
        try {
            statement.executeBatch();
        } catch (BatchUpdateException bue) {
            LOG.error("exec error", bue);
            SQLException cause = bue.getNextException();
            return cause != null ? cause : bue;
        } catch (SQLException e) {
            LOG.error("exec error", e);
            return e;
        } finally {
            ClickHouseBasePlugin.closeStatementAndConnection(statement);
        }
        return null;
    }

    SegmentIdDataWriter(ClickHouseBasePlugin plugin, String query, Integer segmentId, int batchSize) throws SQLException {
        this(plugin, query, batchSize, plugin.getConnection(segmentId).prepareStatement(query));
    }

    SegmentIdDataWriter(ClickHouseBasePlugin plugin, String query, int batchSize) throws SQLException {
        this(plugin, query, batchSize, plugin.getConnection().prepareStatement(query));
    }

    SegmentIdDataWriter(ClickHouseBasePlugin plugin, String query, int batchSize, PreparedStatement statement) throws SQLException {
        if (plugin == null || query == null) {
            throw new IllegalArgumentException("The provided JdbcBasePlugin or SQL query is null");
        }
        this.statement = statement;
        this.batchSize = batchSize;
        statement.unwrap(ClickHousePreparedStatement.class).setBatchSize(batchSize);
    }

    private int rows;
    private PreparedStatement statement;
    private final int batchSize;
}
