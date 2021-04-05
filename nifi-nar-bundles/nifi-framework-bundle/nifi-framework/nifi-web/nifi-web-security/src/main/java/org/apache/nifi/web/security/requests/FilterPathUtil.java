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
package org.apache.nifi.web.security.requests;

import org.apache.nifi.logging.NiFiLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.List;

class FilterPathUtil {
    
    private static final Logger logger = new NiFiLog(LoggerFactory.getLogger(FilterPathUtil.class));
    private static final List<String> BYPASS_URI_PREFIXES = Arrays.asList("/nifi-api/data-transfer", "/nifi-api/site-to-site");

    /**
     * Returns {@code true} if this request is subject to the filter operation, {@code false} if not.
     *
     * @param request the incoming request
     * @return true if this request should be filtered
     */
    public static boolean isSubjectToFilter(HttpServletRequest request, final String filterName) {
        for (String uriPrefix : BYPASS_URI_PREFIXES) {
            if (request.getRequestURI().startsWith(uriPrefix)) {
                logger.debug("Incoming request {} matches filter bypass prefix {}; {} filter is not applied", request.getRequestURI(), uriPrefix, filterName);
                return false;
            }
        }
        return true;
    }
}
