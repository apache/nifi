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

package org.apache.nifi.minifi.c2.integration.test.health;

import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.function.Function;

public class HttpStatusCodeHealthCheck implements HealthCheck<Container> {
    private final Function<Container, String> urlFunction;
    private final int expected;

    public HttpStatusCodeHealthCheck(Function<Container, String> urlFunction, int expected) {
        this.urlFunction = urlFunction;
        this.expected = expected;
    }

    @Override
    public SuccessOrFailure isHealthy(Container target) {
        try {
            int responseCode = openConnection(urlFunction.apply(target)).getResponseCode();
            if (responseCode == expected) {
                return SuccessOrFailure.success();
            } else {
                return SuccessOrFailure.failure("Expected Status code " + expected + " got " + responseCode);
            }
        } catch (IOException e) {
            return SuccessOrFailure.failure("Expected Status code " + expected + " got IOException " + e.getMessage());
        }
    }

    protected HttpURLConnection openConnection(String url) throws IOException {
        return ((HttpURLConnection) new URL(url).openConnection());
    }
}
