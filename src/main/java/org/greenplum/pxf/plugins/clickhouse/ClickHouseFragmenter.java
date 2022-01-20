package org.greenplum.pxf.plugins.clickhouse;


import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.FragmentStats;
import org.greenplum.pxf.plugins.clickhouse.partitioning.ChBasePartition;
import org.greenplum.pxf.plugins.clickhouse.partitioning.PartitionType;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;

import java.util.ArrayList;
import java.util.List;

/**
 * Clickhouse fragmenter
 * <p>
 * Splits the query to allow multiple simultaneous SELECTs
 */
public class ClickHouseFragmenter extends BaseFragmenter {

    private PartitionType partitionType;
    private String column;
    private String range;
    private String interval;

    private List<List<String>> jdbcUrlShardReplicas = new ArrayList<>(4);

    @Override
    public void afterPropertiesSet() {
        // clickhouse jdbcUrl
        String jdbcUrl = configuration.get(ClickHouseBasePlugin.JDBC_URL_PROPERTY_NAME);
        if (StringUtils.isBlank(jdbcUrl)) {
            throw new IllegalArgumentException(String.format(
                    "Required parameter %s is missing or empty in jdbc-site.xml and option %s is not specified in table definition.",
                    ClickHouseBasePlugin.JDBC_URL_PROPERTY_NAME, ClickHouseBasePlugin.JDBC_URL_OPTION_NAME)
            );
        }

        String[] splitUrl = jdbcUrl.split(";", -1);
        if (splitUrl.length > 0){
            for (String url : splitUrl) {
                List<String> urls = BalancedClickhouseDataSource.splitUrl(url);
                jdbcUrlShardReplicas.add(urls);
            }
        }
        if (jdbcUrlShardReplicas.size() == 0) {
            throw new IllegalArgumentException("ClickHouse jdbcUrl Shard Required greater than 0");
        }
        LOG.info("jdbc shard url: " + jdbcUrlShardReplicas.toString());

        String partitionByOption = context.getOption("PARTITION_BY");
        if (partitionByOption == null) return;

        try {
            String[] partitionBy = partitionByOption.split(":");
            column = partitionBy[0];
            partitionType = PartitionType.of(partitionBy[1]);
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new IllegalArgumentException("The parameter 'PARTITION_BY' has incorrect format. The correct format is '<column_name>:{int|date|enum}'");
        }

        range = context.getOption("RANGE");
        interval = context.getOption("INTERVAL");
    }

    /**
     * getFragments() implementation.
     * Note that all partitionType parameters must be verified before calling this procedure.
     *
     * @return a list of getFragmentsMetadata to be passed to PXF segments
     */
    @Override
    public List<Fragment> getFragments() {
        // clickhouse shard replication jdbc url connection fragments
        if (partitionType == null) {// read shard ext storage
            for (List<String> jdbcUrlReplicas : jdbcUrlShardReplicas) {
                ChBasePartition chBasePartition = new ChBasePartition("");
                chBasePartition.setJdbcUrlReplicas(jdbcUrlReplicas);
                fragments.add(new Fragment(context.getDataSource(), chBasePartition));
            }
        } else {// read shard column partition ext storage
            for (List<String> jdbcUrlReplicas : jdbcUrlShardReplicas) {
                List<ChBasePartition> fragmentsMetadata = partitionType.getFragmentsMetadata(column, range, interval);
                for (ChBasePartition fragmentMetadata : fragmentsMetadata) {
                    fragmentMetadata.setJdbcUrlReplicas(jdbcUrlReplicas);
                    fragments.add(new Fragment(context.getDataSource(), fragmentMetadata)); // TODO 打散
                }
            }
        }
        return fragments;
    }

    /**
     * @return fragment stats
     * @throws UnsupportedOperationException ANALYZE for Jdbc plugin is not supported
     */
    @Override
    public FragmentStats getFragmentStats() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("ANALYZE for ClickHouse JDBC plugin is not supported");
    }
}
