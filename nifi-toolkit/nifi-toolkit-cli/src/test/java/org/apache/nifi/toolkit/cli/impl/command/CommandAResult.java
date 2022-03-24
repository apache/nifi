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
package org.apache.nifi.toolkit.cli.impl.command;

import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.api.ReferenceResolver;
import org.apache.nifi.toolkit.cli.api.Referenceable;
import org.apache.nifi.toolkit.cli.api.ResolvedReference;
import org.apache.nifi.toolkit.cli.api.Result;

import java.util.List;

public class CommandAResult implements Result<List<String>>, Referenceable {

    private final List<String> results;

    public CommandAResult(final List<String> results) {
        this.results = results;
    }

    @Override
    public List<String> getResult() {
        return results;
    }

    @Override
    public ReferenceResolver createReferenceResolver(Context context) {
        return new ReferenceResolver() {
            @Override
            public ResolvedReference resolve(CommandOption option, Integer position) {
                if (position != null && position <= results.size()) {
                    return new ResolvedReference(option, position, "CommandA", results.get(position - 1));
                } else {
                    return null;
                }
            }

            @Override
            public boolean isEmpty() {
                return false;
            }
        };
    }

}
