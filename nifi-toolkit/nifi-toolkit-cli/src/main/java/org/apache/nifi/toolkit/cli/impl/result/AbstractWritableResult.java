/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.toolkit.cli.impl.result;

import org.apache.commons.lang3.Validate;
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.api.WritableResult;
import org.apache.nifi.toolkit.cli.impl.util.JacksonUtils;

import java.io.IOException;
import java.io.PrintStream;

/**
 * Base class for writable results that have either JSON or simple output.
 *
 * @param <T> the type of results
 */
public abstract class AbstractWritableResult<T> implements WritableResult<T> {

    protected final ResultType resultType;

    public AbstractWritableResult(final ResultType resultType) {
        this.resultType = resultType;
        Validate.notNull(resultType);
    }

    @Override
    public void write(final PrintStream output) throws IOException {
        if (resultType == ResultType.JSON) {
            writeJsonResult(output);
        } else {
            writeSimpleResult(output);
        }
    }

    protected abstract void writeSimpleResult(PrintStream output)
            throws IOException;

    protected void writeJsonResult(PrintStream output) throws IOException {
        JacksonUtils.write(getResult(), output);
    }

}
