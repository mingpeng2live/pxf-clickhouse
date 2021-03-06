package org.greenplum.pxf.plugins.clickhouse.writercallable;

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

import org.greenplum.pxf.api.OneRow;

import java.sql.SQLException;
import java.util.concurrent.Callable;

/**
 * An object that processes INSERT operation on {@link OneRow} objects
 */
public interface WriterCallable extends Callable<SQLException> {
    /**
     * Pass the next OneRow to this WriterCallable.
     *
     * @param row row
     * @throws IllegalStateException if this WriterCallable must be call()ed before the next call to supply()
     */
    void supply(OneRow row) throws IllegalStateException;

    /**
     * Check whether this WriterCallable must be called
     *
     * @return true if this WriterCallable must be call()ed before the next call to supply(), false otherwise
     */
    boolean isCallRequired();

    /**
     * Execute an INSERT query.
     *
     * @return null or a SQLException that happened when executing the query or if the query was empty (nothing was there to execute)
     *
     * @throws Exception an exception that happened during execution, but that is not related to the execution of the query itself (for instance, it may originate from {@link java.sql.PreparedStatement} close() method)
     */
    @Override
    SQLException call() throws Exception;
}
