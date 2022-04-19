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

package org.apache.nifi.minifi.bootstrap.command;

import static org.apache.nifi.minifi.bootstrap.Status.OK;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Composite runner which can execute multiple commands in a sequential order.
 */
public class CompositeCommandRunner implements CommandRunner {
    final List<CommandRunner> services;

    public CompositeCommandRunner(List<CommandRunner> services) {
        this.services = Optional.ofNullable(services).map(Collections::unmodifiableList).orElse(Collections.emptyList());
    }

    /**
     * Executes the runners in sequential order. Stops on first failure.
     * @param args the input arguments
     * @return the first failed command status code or OK if there was no failure
     */
    @Override
    public int runCommand(String[] args) {
        return services.stream()
            .map(service -> service.runCommand(args))
            .filter(code -> code != OK.getStatusCode())
            .findFirst()
            .orElse(OK.getStatusCode());
    }
}
