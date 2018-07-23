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
import org.apache.nifi.registry.authorization.CurrentUser;
import org.apache.nifi.toolkit.cli.api.ResultType;

import java.io.PrintStream;

/**
 * Result for CurrentUser from registry.
 */
public class CurrentUserResult extends AbstractWritableResult<CurrentUser> {

    private final CurrentUser currentUser;

    public CurrentUserResult(final ResultType resultType, final CurrentUser currentUser) {
        super(resultType);
        this.currentUser = currentUser;
        Validate.notNull(this.currentUser);
    }

    @Override
    public CurrentUser getResult() {
        return currentUser;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) {
        output.println(currentUser.getIdentity());
    }
}
