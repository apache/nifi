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
package org.apache.nifi.web.filter;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.nifi.cluster.coordination.http.replication.RequestReplicator;
import org.apache.nifi.logging.NiFiLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A filter to time requests.
 *
 */
public class TimerFilter implements Filter {

    private static final Logger logger = new NiFiLog(LoggerFactory.getLogger(TimerFilter.class));

    @Override
    public void doFilter(final ServletRequest req, final ServletResponse resp, final FilterChain filterChain)
            throws IOException, ServletException {

        final HttpServletRequest request = (HttpServletRequest) req;

        final long start = System.nanoTime();
        try {
            filterChain.doFilter(req, resp);
        } finally {
            final long stop = System.nanoTime();
            final String requestId = ((HttpServletRequest) req).getHeader(RequestReplicator.REQUEST_TRANSACTION_ID_HEADER);
            final String replicationHeader = ((HttpServletRequest) req).getHeader(RequestReplicator.REPLICATION_INDICATOR_HEADER);
            final boolean validationPhase = RequestReplicator.NODE_CONTINUE.equals(replicationHeader);
            final String requestDescription = validationPhase ? "Validation Phase of Request " + requestId : "Request ID " + requestId;

            logger.debug("{} {} from {} duration for {}: {} millis", request.getMethod(), request.getRequestURL().toString(),
                req.getRemoteHost(), requestDescription, TimeUnit.MILLISECONDS.convert(stop - start, TimeUnit.NANOSECONDS));
        }
    }

    @Override
    public void init(final FilterConfig config) {
    }

    @Override
    public void destroy() {
    }

}
