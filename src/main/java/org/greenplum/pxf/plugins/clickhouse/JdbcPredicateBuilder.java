package org.greenplum.pxf.plugins.clickhouse;

import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.filter.ColumnPredicateBuilder;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.clickhouse.utils.DbProduct;

import java.util.List;


public class JdbcPredicateBuilder extends ColumnPredicateBuilder {

    private final DbProduct dbProduct;

    public JdbcPredicateBuilder(DbProduct dbProduct,
                                List<ColumnDescriptor> tupleDescription) {
        this(dbProduct, "", tupleDescription);
    }

    public JdbcPredicateBuilder(DbProduct dbProduct,
                                String quoteString,
                                List<ColumnDescriptor> tupleDescription) {
        super(quoteString, tupleDescription);
        this.dbProduct = dbProduct;
    }

    @Override
    public String toString() {
        StringBuilder sb = getStringBuilder();
        if (sb.length() > 0) {
            sb.insert(0, " WHERE ");
        }
        return sb.toString();
    }

    @Override
    protected String serializeValue(DataType type, String value) {
        switch (type) {
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT8:
            case REAL:
            case BOOLEAN:
                return value;
            case TEXT:
                return String.format("'%s'",
                        StringUtils.replace(value, "'", "''"));
            case DATE:
                // Date field has different format in different databases
                return dbProduct.wrapDate(value);
            case TIMESTAMP:
                // Timestamp field has different format in different databases
                return dbProduct.wrapTimestamp(value);
            default:
                throw new UnsupportedOperationException(String.format(
                        "Unsupported column type for filtering '%s' ", type.getOID()));
        }
    }
}
