package org.greenplum.pxf.plugins.clickhouse.partitioning;

import lombok.*;
import org.greenplum.pxf.plugins.clickhouse.utils.DbProduct;

import java.util.List;

/**
 * A base class for partition of any type.
 * <p>
 * All partitions use some column as a partition column. It is processed by this class.
 */
@NoArgsConstructor
@RequiredArgsConstructor
public class ChBasePartition implements ChFragmentMetadata {

    /**
     * Column name to use as a partition column. Must not be null
     */
    @Getter
    @NonNull
    protected String column;

    // choose clickhouse shard jdbc connection
    @Getter
    @Setter
    protected List<String> jdbcUrlReplicas;

    /**
     * Generate a range-based SQL constraint
     *
     * @param quotedColumn column name (used as is, thus it should be quoted if necessary)
     * @param range        range to base constraint on
     * @return a pure SQL constraint (without WHERE)
     */
    String generateRangeConstraint(String quotedColumn, String[] range) {
        StringBuilder sb = new StringBuilder(quotedColumn);

        if (range.length == 1) {
            sb.append(" = ").append(range[0]);
        } else if (range[0] == null) {
            sb.append(" < ").append(range[1]);
        } else if (range[1] == null) {
            sb.append(" >= ").append(range[0]);
        } else {
            sb.append(" >= ").append(range[0])
                    .append(" AND ")
                    .append(quotedColumn).append(" < ").append(range[1]);
        }

        return sb.toString();
    }

    public String toSqlConstraint(String quoteString, DbProduct dbProduct) {
        return "";
    }

}
