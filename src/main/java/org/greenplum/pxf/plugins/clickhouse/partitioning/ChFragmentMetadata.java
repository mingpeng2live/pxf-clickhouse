package org.greenplum.pxf.plugins.clickhouse.partitioning;

import org.greenplum.pxf.api.utilities.FragmentMetadata;
import org.greenplum.pxf.plugins.clickhouse.utils.DbProduct;

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
 * Storage of partition constraints for JDBC partitioning feature
 */
public interface ChFragmentMetadata extends FragmentMetadata {

    /**
     * Form a SQL constraint from the metadata of this fragment.
     *
     * @param quoteString a string to quote partition column
     * @param dbProduct   a {@link DbProduct} to wrap constraint values
     * @return a pure SQL constraint (without WHERE)
     */
    String toSqlConstraint(String quoteString, DbProduct dbProduct);
}
