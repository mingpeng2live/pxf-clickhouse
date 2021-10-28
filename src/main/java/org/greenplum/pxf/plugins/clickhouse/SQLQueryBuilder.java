package org.greenplum.pxf.plugins.clickhouse;

import org.greenplum.pxf.api.filter.*;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.clickhouse.partitioning.ChFragmentMetadata;
import org.greenplum.pxf.plugins.clickhouse.utils.DbProduct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

/**
 * SQL query builder.
 * <p>
 * Uses {@link JdbcPredicateBuilder} to get array of filters
 */
public class SQLQueryBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(SQLQueryBuilder.class);
    private static final String SUBQUERY_ALIAS_SUFFIX = ") pxfsubquery"; // do not use AS, Oracle does not like it

    private static final EnumSet<Operator> SUPPORTED_OPERATORS =
            EnumSet.of(
                    Operator.LESS_THAN,
                    Operator.GREATER_THAN,
                    Operator.LESS_THAN_OR_EQUAL,
                    Operator.GREATER_THAN_OR_EQUAL,
                    Operator.EQUALS,
                    Operator.LIKE,
                    Operator.NOT_EQUALS,
                    // TODO: In is not supported?
                    // Operator.IN,
                    Operator.IS_NULL,
                    Operator.IS_NOT_NULL,
                    Operator.NOOP,
                    Operator.AND,
                    Operator.NOT,
                    Operator.OR
            );
    private static final TreeVisitor PRUNER = new SupportedOperatorPruner(SUPPORTED_OPERATORS);
    private static final TreeTraverser TRAVERSER = new TreeTraverser();

    protected final RequestContext context;

    private final DatabaseMetaData databaseMetaData;
    private final DbProduct dbProduct;
    private final List<ColumnDescriptor> columns;
    private final String source;
    private String quoteString;
    private boolean subQueryUsed = false;

    /**
     * Construct a new SQLQueryBuilder
     *
     * @param context  {@link RequestContext}
     * @param metaData {@link DatabaseMetaData}
     * @throws SQLException if some call of DatabaseMetaData method fails
     */
    public SQLQueryBuilder(RequestContext context, DatabaseMetaData metaData) throws SQLException {
        this(context, metaData, null);
    }

    /**
     * Construct a new SQLQueryBuilder
     *
     * @param context  {@link RequestContext}
     * @param metaData {@link DatabaseMetaData}
     * @param subQuery query to run and get results from, instead of using a table name
     * @throws SQLException if some call of DatabaseMetaData method fails
     */
    public SQLQueryBuilder(RequestContext context, DatabaseMetaData metaData, String subQuery) throws SQLException {
        if (context == null) {
            throw new IllegalArgumentException("Provided RequestContext is null");
        }
        this.context = context;
        if (metaData == null) {
            throw new IllegalArgumentException("Provided DatabaseMetaData is null");
        }
        databaseMetaData = metaData;

        dbProduct = DbProduct.getDbProduct(databaseMetaData.getDatabaseProductName());
        columns = context.getTupleDescription();
        // pick the source as either requested table name or a wrapped subquery with an alias
        if (subQuery == null) {
            source = context.getDataSource();
        } else {
            source = String.format("(%s%s", subQuery, SUBQUERY_ALIAS_SUFFIX);
            subQueryUsed = true;
        }

        quoteString = "";
    }

    public SQLQueryBuilder(DatabaseMetaData metaData, RequestContext context) throws SQLException {
        if (context == null) {
            throw new IllegalArgumentException("Provided RequestContext is null");
        }
        this.context = context;

        databaseMetaData = metaData;
        dbProduct = null;

        columns = context.getTupleDescription();

        // pick the source as either requested table name or a wrapped subquery with an alias
        source = context.getDataSource();

        quoteString = "";
    }


    /**
     * Build SELECT query (with "WHERE" and partition constraints).
     *
     * @return Complete SQL query
     */
    public String buildSelectQuery() {
        StringBuilder sb = new StringBuilder("SELECT ")
                .append(buildColumnsQuery())
                .append(" FROM ")
                .append(getSource());

        // Insert regular WHERE constraints
        buildWhereSQL(sb);

        // Insert partition constraints
        buildFragmenterSql(context, dbProduct, quoteString, sb);

        return sb.toString();
    }

    /**
     * Build INSERT query template (field values are replaced by placeholders '?')
     *
     * @return SQL query with placeholders instead of actual values
     */
    public String buildInsertQuery() {
        StringBuilder sb = new StringBuilder();

        sb.append("INSERT INTO ");
        sb.append(source);

        // Insert columns' names
        sb.append("(");
        String fieldDivisor = "";
        for (ColumnDescriptor column : columns) {
            sb.append(fieldDivisor);
            fieldDivisor = ", ";
            sb.append(quoteString).append(column.columnName()).append(quoteString);
        }
        sb.append(")");

        sb.append(" VALUES ");

        // Insert values placeholders
        sb.append("(");
        fieldDivisor = "";
        for (int i = 0; i < columns.size(); i++) {
            sb.append(fieldDivisor);
            fieldDivisor = ", ";
            sb.append("?");
        }
        sb.append(")");

        return sb.toString();
    }

    /**
     * Check whether column names must be quoted and set quoteString if so.
     * <p>
     * Quote string is set to value provided by {@link DatabaseMetaData}.
     *
     * @throws SQLException if some method of {@link DatabaseMetaData} fails
     */
    public void autoSetQuoteString() throws SQLException {
        // Prepare a pattern of characters that may be not quoted
        String extraNameCharacters = databaseMetaData.getExtraNameCharacters();
        LOG.debug("Extra name characters supported by external database: {}", extraNameCharacters);

        extraNameCharacters = extraNameCharacters.replace("-", "\\-");
        Pattern normalCharactersPattern = Pattern.compile("[" + "\\w" + extraNameCharacters + "]+");

        // Check if some column name should be quoted
        boolean mixedCaseNamePresent = false;
        boolean specialCharactersNamePresent = false;
        for (ColumnDescriptor column : columns) {
            // Define whether column name is mixed-case
            // GPDB uses lower-case names if column name was not quoted
            if (column.columnName().toLowerCase() != column.columnName()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Column " + column.columnIndex() + " '" + column.columnName() + "' is mixed-case");
                }
                mixedCaseNamePresent = true;
                break;
            }
            // Define whether column name contains special symbols
            if (!normalCharactersPattern.matcher(column.columnName()).matches()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Column " + column.columnIndex() + " '" + column.columnName() + "' contains special characters");
                }
                specialCharactersNamePresent = true;
                break;
            }
        }

        if (specialCharactersNamePresent || (mixedCaseNamePresent &&
                !databaseMetaData.supportsMixedCaseIdentifiers())) {
            quoteString = databaseMetaData.getIdentifierQuoteString();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Quotation auto-enabled; quote string set to '" + quoteString + "'");
            }
        }
    }

    /**
     * Set quoteString to value provided by {@link DatabaseMetaData}.
     *
     * @throws SQLException if some method of {@link DatabaseMetaData} fails
     */
    public void forceSetQuoteString() throws SQLException {
        quoteString = databaseMetaData.getIdentifierQuoteString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Quotation force-enabled; quote string set to '" + quoteString + "'");
        }
    }

    /**
     * Builds the columns queried in a SELECT query
     *
     * @return the columns query
     */
    protected String buildColumnsQuery() {
        return this.columns.stream()
                .filter(ColumnDescriptor::isProjected)
                .map(c -> quoteString + c.columnName() + quoteString)
                .collect(Collectors.joining(", "));
    }

    /**
     * Returns the source table for the SELECT query
     *
     * @return the source table for the SELECT query
     */
    protected String getSource() {
        return source;
    }

    /**
     * Returns the JdbcPredicateBuilder that generates the predicate for this
     * database
     *
     * @return the JdbcPredicateBuilder
     */
    protected JdbcPredicateBuilder getPredicateBuilder() {
        return new JdbcPredicateBuilder(
                dbProduct,
                quoteString,
                context.getTupleDescription());
    }

    /**
     * Return the pruner for the parsed expression tree
     *
     * @return the tree pruner
     */
    protected TreeVisitor getPruner() {
        return PRUNER;
    }

    /**
     * Insert WHERE constraints into a given query.
     * Note that if filter is not supported, query is left unchanged.
     *
     * @param query SQL query to insert constraints to. The query may may contain other WHERE statements
     */
    private void buildWhereSQL(StringBuilder query) {
        if (!context.hasFilter()) return;

        JdbcPredicateBuilder jdbcPredicateBuilder = getPredicateBuilder();

        try {
            // Parse the filter string into a expression tree Node
            Node root = new FilterParser().parse(context.getFilterString());
            // Prune the parsed tree with the provided pruner and then
            // traverse the tree with the JDBC predicate builder to produce a predicate
            TRAVERSER.traverse(root, getPruner(), jdbcPredicateBuilder);
            // No exceptions were thrown, change the provided query
            query.append(jdbcPredicateBuilder.toString());
        } catch (Exception e) {
            LOG.debug("WHERE clause is omitted: " + e.toString());
            // Silence the exception and do not insert constraints
        }
    }

    /**
     * Insert fragment constraints into the SQL query.
     *
     * @param context     RequestContext of the fragment
     * @param dbProduct   Database product (affects the behaviour for DATE partitions)
     * @param quoteString String to use as quote for column identifiers
     * @param query       SQL query to insert constraints to. The query may may contain other WHERE statements
     */
    public void buildFragmenterSql(RequestContext context, DbProduct dbProduct, String quoteString, StringBuilder query) {
        if (context.getOption("PARTITION_BY") == null || context.getFragmentMetadata() == null) {
            return;
        }

        // determine if we need to add WHERE statement if not a single WHERE is in the query
        // or subquery is used and there are no WHERE statements after subquery alias
        int startIndexToSearchForWHERE = 0;
        if (subQueryUsed) {
            startIndexToSearchForWHERE = query.indexOf(SUBQUERY_ALIAS_SUFFIX);
        }
        if (query.indexOf("WHERE", startIndexToSearchForWHERE) < 0) {
            query.append(" WHERE ");
        } else {
            query.append(" AND ");
        }

        ChFragmentMetadata fragmentMetadata = context.getFragmentMetadata();
        String fragmentSql = fragmentMetadata.toSqlConstraint(quoteString, dbProduct);

        query.append(fragmentSql);
    }
}
