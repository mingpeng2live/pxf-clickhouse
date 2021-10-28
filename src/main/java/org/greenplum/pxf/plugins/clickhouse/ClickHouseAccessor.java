package org.greenplum.pxf.plugins.clickhouse;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.security.SecureLogin;
import org.greenplum.pxf.plugins.clickhouse.utils.ChConnectionManager;
import org.greenplum.pxf.plugins.clickhouse.writercallable.SegmentIdSortUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.response.ClickHouseResultSet;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.*;


public class ClickHouseAccessor extends ClickHouseBasePlugin implements Accessor {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseAccessor.class);

    private Statement statementRead = null;
    private ClickHouseResultSet clickHouseResultSet = null;

    private SegmentIdSortUtils segmentIdSortUtils = null;


    /**
     * Creates a new instance of the JdbcAccessor
     */
    public ClickHouseAccessor() {
        super();
    }

    /**
     * Creates a new instance of accessor with provided connection manager.
     *
     * @param connectionManager connection manager
     * @param secureLogin       the instance of the secure login
     */
    ClickHouseAccessor(ChConnectionManager connectionManager, SecureLogin secureLogin) {
        super(connectionManager, secureLogin);
    }

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        LOG.info("segmentColumnName: {} segmentColumnIndex: {} columnSize: {} columns: {}", segmentColumnName, segmentColumnIndex, columns.size(), columns);
    }

    /**
     * openForRead() implementation
     * Create query, open JDBC connection, execute query and store the result into resultSet
     *
     * @return true if successful
     * @throws SQLException        if a database access error occurs
     * @throws SQLTimeoutException if a problem with the connection occurs
     */
    @Override
    public boolean openForRead() throws SQLException, SQLTimeoutException {
        if (statementRead != null && !statementRead.isClosed()) {
            return true;
        }

        Connection connection = super.getConnection(-1);
        SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(context, connection.getMetaData(), getQueryText());

        // Build SELECT query
        if (quoteColumns == null) {
            sqlQueryBuilder.autoSetQuoteString();
        } else if (quoteColumns) {
            sqlQueryBuilder.forceSetQuoteString();
        }
        // Read variables
        String queryRead = sqlQueryBuilder.buildSelectQuery();

        LOG.info("Select query: {} Filter: {} fetchSize: {}", queryRead, context.getFilterString(), fetchSize);

        // Execute queries
        statementRead = connection.createStatement();
        statementRead.setFetchSize(fetchSize);

        if (queryTimeout != null) {
            LOG.debug("Setting query timeout to {} seconds", queryTimeout);
            statementRead.setQueryTimeout(queryTimeout);
        }
        clickHouseResultSet = statementRead.executeQuery(queryRead).unwrap(ClickHouseResultSet.class);

        return true;
    }

    /**
     * readNextObject() implementation
     * Retreive the next tuple from resultSet and return it
     *
     * @return row
     * @throws SQLException if a problem in resultSet occurs
     */
    @Override
    public OneRow readNextObject() throws SQLException {
        if (clickHouseResultSet.next()) {
            return new OneRow(clickHouseResultSet);
        }
        return null;
    }

    /**
     * closeForRead() implementation
     */
    @Override
    public void closeForRead() throws SQLException {
        closeStatementAndConnection(statementRead);
    }

    /**
     * openForWrite() implementation
     * Create query template and open JDBC connection
     *
     * @return true if successful
     * @throws SQLException        if a database access error occurs
     * @throws SQLTimeoutException if a problem with the connection occurs
     */
    @Override
    public boolean openForWrite() throws SQLException, SQLTimeoutException {
        if (queryName != null) {
            throw new IllegalArgumentException("specifying query name in data path is not supported for JDBC writable external tables");
        }

        Connection connection = super.getConnection();
        SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(connection.getMetaData(), context);

        // Build INSERT query
        if (quoteColumns == null) {
            sqlQueryBuilder.autoSetQuoteString();
        } else if (quoteColumns) {
            sqlQueryBuilder.forceSetQuoteString();
        }
        // Write variables
        String queryWrite = sqlQueryBuilder.buildInsertQuery();

        // Process batchSize
        if (!connection.getMetaData().supportsBatchUpdates()) {
            if ((batchSizeIsSetByUser) && (batchSize > 1)) {
                throw new SQLException("The external database does not support batch updates");
            } else {
                batchSize = 1;
            }
        }

        // Process poolSize
        if (poolSize < 1) {
            poolSize = Runtime.getRuntime().availableProcessors();
            LOG.info("The POOL_SIZE is set to the number of CPUs available ({})", poolSize);
        }

        LOG.info("Insert query: {} batchSize: {} poolSize: {} jdbcShardSize: {}", queryWrite, batchSize, poolSize, jdbcUrlShardReplicas.size());

        // Setup SegmentId Writer Shard
        segmentIdSortUtils = new SegmentIdSortUtils(this, queryWrite, batchSize, poolSize);

        try {
            closeConnection(connection);
        } catch (SQLException e) {
            LOG.error(String.format("Exception when closing connection %s", connection), e);
            throw e;
        }
        return true;
    }

     /**
     * writeNextObject() implementation
     * <p>
     * If batchSize is not 0 or 1, add a tuple to the batch of statementWrite
     * Otherwise, execute an INSERT query immediately
     * <p>
     * In both cases, a {@link PreparedStatement} is used
     *
     * @param row one row
     * @return true if successful
     * @throws SQLException           if a database access error occurs
     * @throws IOException            if the data provided by {@link ClickHouseResolver} is corrupted
     * @throws ClassNotFoundException if pooling is used and the JDBC driver was not found
     * @throws IllegalStateException  if writerCallableFactory was not properly initialized
     * @throws Exception              if it happens in writerCallable.call()
     */
    @Override
    public boolean writeNextObject(OneRow row) throws Exception {
        // TODO Save according to segmentId of the line
        segmentIdSortUtils.supply(row);
        return true;
    }

    /**
     * closeForWrite() implementation
     *
     * @throws Exception if it happens in writerCallable.call() or due to runtime errors in thread pool
     */
    @Override
    public void closeForWrite() throws Exception {
        segmentIdSortUtils.close();
    }


    /**
     * Gets the text of the query by reading the file from the server configuration directory. The name of the file
     * is expected to be the same as the name of the query provided by the user and have extension ".sql"
     *
     * @return text of the query
     */
    private String getQueryText() {
        if (StringUtils.isBlank(queryName)) {
            return null;
        }
        // read the contents of the file holding the text of the query with a given name
        String serverDirectory = context.getConfiguration().get(ConfigurationFactory.PXF_CONFIG_SERVER_DIRECTORY_PROPERTY);
        if (StringUtils.isBlank(serverDirectory)) {
            throw new IllegalStateException("No server configuration directory found for server " + context.getServerName());
        }

        String queryText;
        try {
            File queryFile = new File(serverDirectory, queryName + ".sql");
            if (LOG.isDebugEnabled()) {
                LOG.debug("Reading text of query={} from {}", queryName, queryFile.getCanonicalPath());
            }
            queryText = FileUtils.readFileToString(queryFile, Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to read text of query %s : %s", queryName, e.getMessage()), e);
        }
        if (StringUtils.isBlank(queryText)) {
            throw new RuntimeException(String.format("Query text file is empty for query %s", queryName));
        }

        // Remove one or more semicolons followed by optional blank space
        // happening at the end of the query
        queryText = queryText.replaceFirst("(;+\\s*)+$", "");

        return queryText;
    }

}
