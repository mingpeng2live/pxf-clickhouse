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

import org.apache.commons.lang3.StringUtils;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.response.ClickHouseColumnInfo;
import ru.yandex.clickhouse.response.ClickHouseResultSet;
import ru.yandex.clickhouse.util.ClickHouseValueFormatter;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Clickhouse tables resolver
 */
public class ClickHouseResolver extends ClickHouseBasePlugin implements Resolver {
    private static final Set<DataType> DATATYPES_SUPPORTED = EnumSet.of(
            DataType.VARCHAR,
            DataType.BPCHAR,
            DataType.TEXT,
            DataType.BYTEA,
            DataType.BOOLEAN,
            DataType.INTEGER,
            DataType.FLOAT8,
            DataType.REAL,
            DataType.BIGINT,
            DataType.SMALLINT,
            DataType.NUMERIC,
            DataType.TIMESTAMP,
            DataType.DATE
    );

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseResolver.class);

    private Map<String, ColumnInfo> columnInfoMap = new ConcurrentHashMap<>();
    private class ColumnInfo {
        public Integer columnIndex;
        public ClickHouseColumnInfo chColumnInfo;
        public ColumnInfo(Integer columnIndex, ClickHouseColumnInfo chColumnInfo) {
            this.columnIndex = columnIndex;
            this.chColumnInfo = chColumnInfo;
        }
    }

    /**
     * getFields() implementation
     *
     * @param row one row
     * @throws SQLException if the provided {@link OneRow} object is invalid
     */
    @Override
    public List<OneField> getFields(OneRow row) throws SQLException {
        ClickHouseResultSet result = (ClickHouseResultSet) row.getData();
        if (columnInfoMap.isEmpty()) {
            int index = 0;
            List<ClickHouseColumnInfo> chColumns = result.getColumns();
            for (ClickHouseColumnInfo column : chColumns) {
                columnInfoMap.put(column.getColumnName(), new ColumnInfo(++index, column));
            }
        }

        List<OneField> fields = new ArrayList<>(columns.size());
        for (ColumnDescriptor column : columns) {
            Object value;

            OneField oneField = new OneField();
            oneField.type = column.columnTypeCode();

            fields.add(oneField);

            /*
             * Non-projected columns get null values
             */
            if (!column.isProjected()) continue;

            // clickhouse columnInfo
            ColumnInfo columnInfo = columnInfoMap.get(column.columnName());
            int columnIndex = columnInfo.columnIndex;

            if (columnInfo.chColumnInfo.isArray()) {
                oneField.type = DataType.TEXT.getOID();
                value = getArrayString(columnInfo, result);
            } else {
                switch (DataType.get(oneField.type)) {
                    case INTEGER:
                        value = result.getInt(columnIndex);
                        break;
                    case FLOAT8:
                        value = result.getDouble(columnIndex);
                        break;
                    case REAL:
                        value = result.getFloat(columnIndex);
                        break;
                    case BIGINT:
                        value = result.getLong(columnIndex);
                        break;
                    case SMALLINT:
                        value = result.getShort(columnIndex);
                        break;
                    case BOOLEAN:
                        value = result.getBoolean(columnIndex);
                        break;
                    case BYTEA:
                        value = result.getBytes(columnIndex);
                        break;
                    case VARCHAR:
                    case BPCHAR:
                    case TEXT:
                    case NUMERIC:
                        value = result.getString(columnIndex);
                        break;
                    case DATE:
                        value = result.getDate(columnIndex);
                        break;
                    case TIMESTAMP:
                        value = result.getTimestamp(columnIndex);
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                String.format("Field type '%s' (column '%s') is not supported",
                                        DataType.get(oneField.type),
                                        column));
                }
            }
            oneField.val = result.wasNull() ? null : value;
        }

        return fields;
    }


    private static Object getArrayString(ColumnInfo columnInfo, ResultSet result) throws SQLException {
        Integer columnIndex = columnInfo.columnIndex;
        JDBCType jdbcType = columnInfo.chColumnInfo.getArrayBaseType().getJdbcType();

        if (jdbcType == JDBCType.INTEGER ||
                jdbcType == JDBCType.BIGINT ||
                jdbcType == JDBCType.FLOAT ||
                jdbcType == JDBCType.DOUBLE ||
                jdbcType == JDBCType.NUMERIC ||
                jdbcType == JDBCType.DECIMAL ||
                jdbcType == JDBCType.SMALLINT ||
                jdbcType == JDBCType.REAL ||
                jdbcType == JDBCType.TINYINT // bool 1,0 可以处理为bool 数组
        ) {
            String val = result.getString(columnIndex); // default null is []
            if (val != null) {
                val = val.substring(1, val.length() - 1);
                return val.length() == 0 ? null : "{" + val + "}";
            }
            return null;
        }

        StringBuffer resultStr = new StringBuffer("{");
        if (jdbcType == JDBCType.VARCHAR) {
            String[] texts = (String[]) result.getArray(columnIndex).getArray();
            for (String item : texts) {
                if (item != null) {
                    if (item.contains("\\")) {
                        item = item.replaceAll("\\\\", "\\\\\\\\");
                    }
                    if (item.contains("\"")) {
                        item = item.replaceAll("\"", "\\\\\"");
                    }
                    resultStr.append("\"").append(item).append("\"").append(",");
                } else {
                    resultStr.append("NULL").append(",");
                }
            }
        } else if (jdbcType == JDBCType.DATE) {
            Date[] dates = (Date[]) result.getArray(columnIndex).getArray();
            for (Date item : dates) {
                resultStr.append(item == null ? "NULL" : item).append(",");
            }
        } else if (jdbcType == JDBCType.TIMESTAMP) {
            Timestamp[] timestamps = (Timestamp[]) result.getArray(columnIndex).getArray();
            for (Timestamp item : timestamps) {
                if (item != null) {
                    resultStr.append("\"").append(item).append("\"").append(",");
                } else {
                    resultStr.append("NULL").append(",");
                }
            }
        }
        resultStr.deleteCharAt(resultStr.length() - 1);
        return resultStr.length() == 0 ? null : resultStr.append("}").toString();
    }


    /**
     * setFields() implementation
     *
     * @param record List of fields
     * @return OneRow with the data field containing a List of fields
     * OneFields are not reordered before being passed to Accessor; at the
     * moment, there is no way to correct the order of the fields if it is not.
     * In practice, the 'record' provided is always ordered the right way.
     * @throws UnsupportedOperationException if field of some type is not supported
     */
    @Override
    public OneRow setFields(List<OneField> record) throws UnsupportedOperationException {
        Object key = null;
        if (shardWriterEnabled && jdbcUrlShardReplicas.size() > 1) { // 在分片数大于1 且需要按照segmentId 分开写入对应分片，则需要得到当前数据分片key
            OneField oneField = record.get(segmentColumnIndex);
            key = oneField.val;
        }


        int columnIndex = 0;
//        LOG.info("gp row: " + record.toString());
        StringBuilder rowStr = new StringBuilder();
        for (OneField oneField : record) {
            if (columnIndex > 0) {
                rowStr.append("\t");
            }

            ColumnDescriptor column = columns.get(columnIndex++);

            DataType oneFieldType = DataType.get(oneField.type);
            DataType columnType = column.getDataType();

            //dbColumnTypeCode=114 json,  dbColumnTypeCode=221747 roaringbitmap
            // SELECT '\x3a3000000100000000000000100000000100'::ROARINGBITMAP;
            boolean flag = column.columnTypeCode() == 114 || column.columnTypeCode() == 221747;
            if (!flag && !DATATYPES_SUPPORTED.contains(oneFieldType)) {
                throw new UnsupportedOperationException(
                        String.format("Field type '%s' (column '%s') is not supported",
                                oneFieldType, column));
            }


            String valDebug = "";
            if (LOG.isDebugEnabled()) {
                if (oneField.val == null) {
                    valDebug = "null";
                } else if (oneFieldType == DataType.BYTEA) {
                    valDebug = String.format("'{}'", new String((byte[]) oneField.val));
                } else {
                    valDebug = String.format("'{}'", oneField.val.toString());
                }
                LOG.info("Column {} valueType: type {}, columnType {}, content {}", columnIndex, oneFieldType, column.columnTypeName(), valDebug);
            }


            if (oneField.val == null || oneField.val.toString().equalsIgnoreCase("null")) {
                rowStr.append(ClickHouseValueFormatter.formatNull());
                continue;
            }

            if (oneFieldType == DataType.TEXT && (columnType != DataType.TEXT)) {
                if (column.columnTypeName().startsWith("_")) {
                    if (oneField.val.toString().equalsIgnoreCase("{}")) {
                        rowStr.append(ClickHouseValueFormatter.formatNull());
                    } else {
                        rowStr.append("[");
                        arrayValues(column, (String) oneField.val, rowStr);
                        rowStr.append("]");
                    }
                } else if (columnType == DataType.BOOLEAN) {
                    rowStr.append("true".equals(oneField.val) ? "1" : "0");
                } else {
                    rowStr.append(oneField.val);
                }
            } else {
                if (oneFieldType == DataType.BOOLEAN) {
                    rowStr.append(ClickHouseValueFormatter.formatBoolean((Boolean) oneField.val));
                } else {
                    rowStr.append(oneField.val);
                }
            }
        }
//        LOG.info("ck row: " + rowStr.toString());

        return new OneRow(key, rowStr);
    }


    private static void arrayValues(ColumnDescriptor column, String val, StringBuilder rowStr) {
        String type = column.columnTypeName();
        val = val.substring(1, val.length() - 1);
        if (type.startsWith("_int") || type.startsWith("_bigint") || type.startsWith("_float") || type.startsWith("_serial")
                || type.startsWith("_double") || type.startsWith("_numeric") || type.startsWith("_decimal")
        ) {
            rowStr.append(val);
        } else if ("_timestamp".equalsIgnoreCase(type)) {
            appendStr(val, ",", rowStr, true, true, false);
        } else if ("_date".equalsIgnoreCase(type)) {
            appendStr(val, ",", rowStr, true, false, false);
        } else if ("_bool".equalsIgnoreCase(type)) {
            appendStr(val, ",", rowStr, false, false, true);
        } else if ("_text".equalsIgnoreCase(type) || type.startsWith("_json")) {
            appendStr(val, rowStr);
        }
    }


    public static void appendStr(String str, StringBuilder rowStr) {
        boolean match = false, flag = false;
        int i = 0, start = 0, count = 0, len = str.length();

        while (i < len) {
            if ((i > 0 && str.charAt(i) == '"' && str.charAt(i - 1) != '\\')
                    || (i == 0 && str.charAt(i) == '"')) {
                flag = true;
                if (match) {
                    appendItem(str, rowStr, i, start, count);
                    count++;
                    match = false;
                    flag = false; // 标志完成一次切分
                }
                start = ++i;
                if (i < len && str.charAt(i) == '"') { // 前一位为" 后一位也为" 则也是匹配中
                    match = true;
                }
                continue;
            }
            if (!flag && str.charAt(i) == ',') { // flag false 逻辑切分互斥
                if (match) {
                    appendItem(str, rowStr, i, start, count);
                    count++;
                    match = false;
                }
                start = ++i;
                continue;
            }
            match = true;
            i++;
        }
        if (match) {
            appendItem(str, rowStr, i, start, count);
        }
    }

    private static void appendItem(String str, StringBuilder rowStr, int i, int start, int count) {
        if (count > 0) {
            rowStr.append(",");
        }
        String value = str.substring(start, i);
        if (!"NULL".equalsIgnoreCase(value)) {
            rowStr.append("\'");
            if (value.contains("\\\"")) {
                value = value.replaceAll("\\\\\"", "\"");
            }
            if (value.contains("\\")) {
                value = value.replaceAll("\\\\+", "\\\\");
            }
            rowStr.append(value);
            rowStr.append('\'');
        } else {
            rowStr.append("NULL");
        }
    }


    public static void appendStr(String str, String sepa, StringBuilder rowStr, boolean quote,
                                 boolean trimStartEndChar, boolean replaceBoolValue) {
        boolean match = false;
        int i = 0, start = 0, count = 0, len = str.length();
        while (i < len) {
            if (sepa.indexOf(str.charAt(i)) >= 0) {
                if (match) {
                    appendItem(str, rowStr, quote, i, start, count, trimStartEndChar, replaceBoolValue);
                    count++;
                    match = false;
                }
                start = ++i;
                continue;
            }
            match = true;
            i++;
        }
        if (match) {
            appendItem(str, rowStr, quote, i, start, count, trimStartEndChar, replaceBoolValue);
        }
    }

    private static void appendItem(String str, StringBuilder rowStr, boolean quote, int i, int start, int count,
                                   boolean trimStartEndChar, boolean replaceBoolValue) {
        if (count > 0) {
            rowStr.append(",");
        }
        String value = getSubString(str, trimStartEndChar, replaceBoolValue, i, start);
        if (!"NULL".equalsIgnoreCase(value)) {
            if (quote) {
                rowStr.append("\'");
            }
            rowStr.append(value);
            if (quote) {
                rowStr.append('\'');
            }
        } else {
            rowStr.append("NULL");
        }
    }


    /**
     * Decode OneRow object and pass all its contents to a PreparedStatement
     *
     * @param row       one row
     * @param statement PreparedStatement
     * @throws IOException  if data in a OneRow is corrupted
     * @throws SQLException if the given statement is broken
     */
    @SuppressWarnings("unchecked")
    public static void decodeOneRowToPreparedStatement(OneRow row, PreparedStatement statement, List<ColumnDescriptor> columns) throws IOException, SQLException {
        // This is safe: OneRow comes from JdbcResolver
        List<OneField> tuple = (List<OneField>) row.getData();
        for (int i = 1; i <= tuple.size(); i++) {
            int index = i - 1;
            OneField field = tuple.get(index);

            ColumnDescriptor column = columns.get(index);
            if (column.columnTypeName().startsWith("_")) {
                if (field.val == null || field.val.toString().length() == 2
                        || field.val.toString().equalsIgnoreCase("null")) {
                    statement.setNull(i, Types.ARRAY);
                } else {
                    setArray(statement, i, column, field);
                }
            } else {
                switch (DataType.get(field.type)) {
                    case INTEGER:
                        if (field.val == null) {
                            statement.setNull(i, Types.INTEGER);
                        } else {
                            statement.setInt(i, (int) field.val);
                        }
                        break;
                    case BIGINT:
                        if (field.val == null) {
                            statement.setNull(i, Types.INTEGER);
                        } else {
                            statement.setLong(i, (long) field.val);
                        }
                        break;
                    case SMALLINT:
                        if (field.val == null) {
                            statement.setNull(i, Types.INTEGER);
                        } else {
                            statement.setShort(i, (short) field.val);
                        }
                        break;
                    case REAL:
                        if (field.val == null) {
                            statement.setNull(i, Types.FLOAT);
                        } else {
                            statement.setFloat(i, (float) field.val);
                        }
                        break;
                    case FLOAT8:
                        if (field.val == null) {
                            statement.setNull(i, Types.DOUBLE);
                        } else {
                            statement.setDouble(i, (double) field.val);
                        }
                        break;
                    case BOOLEAN:
                        if (field.val == null) {
                            statement.setNull(i, Types.BOOLEAN);
                        } else {
                            statement.setBoolean(i, (boolean) field.val);
                        }
                        break;
                    case NUMERIC:
                        if (field.val == null) {
                            statement.setNull(i, Types.NUMERIC);
                        } else {
                            statement.setBigDecimal(i, (BigDecimal) field.val);
                        }
                        break;
                    case VARCHAR:
                    case BPCHAR:
                    case TEXT:
                        if (field.val == null) {
                            statement.setNull(i, Types.VARCHAR);
                        } else {
                            statement.setString(i, (String) field.val);
                        }
                        break;
                    case BYTEA:
                        if (field.val == null) {
                            statement.setNull(i, Types.BINARY);
                        } else {
                            statement.setBytes(i, (byte[]) field.val);
                        }
                        break;
                    case TIMESTAMP:
                        if (field.val == null) {
                            statement.setNull(i, Types.TIMESTAMP);
                        } else {
                            statement.setTimestamp(i, (Timestamp) field.val);
                        }
                        break;
                    case DATE:
                        if (field.val == null) {
                            statement.setNull(i, Types.DATE);
                        } else {
                            statement.setDate(i, (Date) field.val);
                        }
                        break;
                    default:
                        throw new IOException("The data tuple from JdbcResolver is corrupted");
                }
            }
        }
    }

    private static void setArray(PreparedStatement statement, int i, ColumnDescriptor column, OneField field) throws IOException, SQLException {
        Array array = statement.getConnection().createArrayOf("String", getArrayValues(column, field));
        statement.setArray(i, array);
    }

    private static String[] getArrayValues(ColumnDescriptor column, OneField field) throws IOException {
        String val = (String) field.val;
        String type = column.columnTypeName();

        val = val.substring(1, val.length() - 1);
        if (type.startsWith("_int") || type.startsWith("_bigint") || type.startsWith("_float") || type.startsWith("_serial")
                || type.startsWith("_double") || type.startsWith("_numeric") || type.startsWith("_decimal")
                || "_text".equalsIgnoreCase(type) || "_date".equalsIgnoreCase(type)
        ) {
            if ("_text".equalsIgnoreCase(type) && val.contains("\"")) {
                return splitStrArray(val);
            }
            return StringUtils.split(val, ",");
        } else if ("_timestamp".equalsIgnoreCase(type)) {
            return split(val, ",", true, false);
        } else if (type.startsWith("_json")) {
            return splitStrArray(val);
        } else if ("_bool".equalsIgnoreCase(type)) {
            return split(val, ",", false, true);
        }
        return new String[0];
    }


    public static String[] splitStrArray(String str) {
        boolean match = false, flag = false;
        int i = 0, start = 0, len = str.length();

        final List<String> list = new ArrayList<>();
        while (i < len) {
            if ((i > 0 && str.charAt(i) == '"' && str.charAt(i - 1) != '\\')
                    || (i == 0 && str.charAt(i) == '"')) {
                flag = true;
                if (match) {
                    list.add(getSubString(str, i, start));
                    match = false;
                    flag = false; // 标志完成一次切分
                }
                start = ++i;
                if (i < len && str.charAt(i) == '"') { // 前一位为" 后一位也为" 则也是匹配中
                    match = true;
                }
                continue;
            }
            if (!flag && str.charAt(i) == ',') { // flag false 逻辑切分互斥
                if (match) {
                    list.add(getSubString(str, i, start));
                    match = false;
                }
                start = ++i;
                continue;
            }
            match = true;
            i++;
        }
        if (match) {
            list.add(getSubString(str, i, start));
        }
        return list.toArray(new String[list.size()]);
    }

    private static String getSubString(String str, int i, int start) {
        String sub = str.substring(start, i);
        if (sub.equalsIgnoreCase("NULL")) {
            return "NULL";
        }
        if (sub.contains("\\\"")) {
            sub = sub.replaceAll("\\\\\"", "\"");
        }
        if (sub.contains("\\")) {
            sub = sub.replaceAll("\\\\+", "\\\\");
        }
        return ClickHouseValueFormatter.formatString(sub);
    }


    public static String[] split(String str, String sepa, boolean trimStartEndChar, boolean replaceBoolValue) {
        boolean match = false;
        int i = 0, start = 0, len = str.length();
        final List<String> list = new ArrayList<>();
        while (i < len) {
            if (sepa.indexOf(str.charAt(i)) >= 0) {
                if (match) {
                    list.add(getSubString(str, trimStartEndChar, replaceBoolValue, i, start));
                    match = false;
                }
                start = ++i;
                continue;
            }
            match = true;
            i++;
        }
        if (match) {
            list.add(getSubString(str, trimStartEndChar, replaceBoolValue, i, start));
        }
        return list.toArray(new String[list.size()]);
    }

    private static String getSubString(String str, boolean trimStartEndChar, boolean replaceBoolValue, int i, int start) {
        String sub = str.substring(start, i);
        if (trimStartEndChar) {
            sub = sub.substring(1, sub.length() - 1);
        } else if (replaceBoolValue) {
            if (sub.equalsIgnoreCase("t")) {
                sub = "1";
            } else if (sub.equalsIgnoreCase("f")) {
                sub = "0";
            }
        }
        return sub;
    }

}
