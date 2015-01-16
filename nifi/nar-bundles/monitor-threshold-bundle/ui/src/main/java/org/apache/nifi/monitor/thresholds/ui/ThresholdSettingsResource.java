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
package org.apache.nifi.monitor.thresholds.ui;
/*
 * NOTE: rule is synonymous with threshold
 */

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.nifi.web.NiFiWebContext;

/**
 * MonitorThreshold REST Web Service (superclass)
 */
public class ThresholdSettingsResource {

    @Context
    protected UriInfo context;

    protected static final Logger logger = LoggerFactory.getLogger(ThresholdSettingsResource.class);
    protected List<String> validation_error_list = new ArrayList<>();

    protected static final String DUPLICATE_ATTRIBUTE_NAME = "Please enter a unique Attribute Name.  An attribute with this name already exists: ";
    protected static final String INVALID_ATTRIBUTE_NAME = "Attribute Name must contain a value.";
    protected static final String INVALID_RULE_ID = "Please enter a value.";
    protected static final String INVALID_SIZE = "Please enter an integer Size value in bytes. Punctuation (including decimals/commas) and other forms of text are not allowed.  Max allowed size is 9223372036854775807.";
    protected static final String INVALID_COUNT = "Please enter an integer File Count. Punctuation (including decimals/commas) and other forms of text are not allowed. Max allowed size is 2147483647.";
    protected static final String GENERAL_ERROR = "A general error has occurred.  Detailed Error Message: ";
    protected static final String DUPLICATE_VALUE = "Please enter a unique value.  A threshold with this value already exists: ";

    protected static final String PROCID = "procid";
    protected static final String ID = "id";

    protected static final String ATTRIBUTE_UUID = "attributeuuid";//uuid of the attribute selected
    protected static final String ATTRIBUTENAME = "attributename";//name of the attribute selected
    protected static final String RULEUUID = "ruuid";
    protected static final String RULEVALUE = "attributevalue";
    protected static final String SIZE = "size";//rule/threshold size
    protected static final String COUNT = "count";//rule/threshold count

    /**
     * Creates a new instance of ThresholdSettingsResource
     */
    public ThresholdSettingsResource() {
    }

    /**
     *
     * @param response
     * @return
     */
    protected ResponseBuilder noCache(ResponseBuilder response) {
        CacheControl cacheControl = new CacheControl();
        cacheControl.setPrivate(true);
        cacheControl.setNoCache(true);
        cacheControl.setNoStore(true);
        return response.cacheControl(cacheControl);
    }

    /**
     * Returns a standard web response with a status 200.
     *
     * @param value
     * @return
     */
    protected ResponseBuilder generateOkResponse(String value) {
        ResponseBuilder response = Response.ok(value);
        return noCache(response);
    }

    /**
     * Validates a number. If the string value cannot be parsed as a long then
     * false is returned.
     *
     * @param value
     * @return
     */
    protected boolean validateStringAsLong(String value) {
        try {
            Long.parseLong(value);
        } catch (NumberFormatException ex) {
            logger.error(ex.getMessage());
            return false;
        }
        return true;
    }

    /**
     * Validates a number. If the string value cannot be parsed as an int then
     * false is returned.
     *
     * @param value
     * @return
     */
    protected boolean validateStringAsInt(String value) {
        try {
            Integer.parseInt(value);
        } catch (NumberFormatException ex) {
            logger.error(ex.getMessage());
            return false;
        }
        return true;
    }

    /**
     * Validates a number. If the string value cannot be parsed as a float then
     * false is returned.
     *
     * @param value
     * @return
     */
    protected boolean validateStringAsFloat(String value) {
        try {
            Float.parseFloat(value);
        } catch (NumberFormatException ex) {
            logger.error(ex.getMessage());
            return false;
        }
        return true;
    }

    protected ThresholdsConfigFile getConfigFile(HttpServletRequest request, ServletContext servletContext) {
        //logger.debug("Running ThresholdSettingsResource: getConfigFile(...).");
        NiFiWebContext nifiContext = (NiFiWebContext) servletContext.getAttribute("nifi-web-context");
        ThresholdsConfigFile file = new ThresholdsConfigFile(nifiContext, request);
        return file;
    }

    protected Boolean getIsAsc(String sord) {
        Boolean isAsc = true;
        if (sord.compareTo("desc") == 0) {
            isAsc = false;
        }
        return isAsc;
    }

    protected BigInteger getBigIntValueOf(String value) {
        Long _value = Long.valueOf(value);
        return BigInteger.valueOf(_value);
    }

    protected String setValidationErrorMessage(List<String> messages) {
        String result = new String();
        for (String message : messages) {
            result += "<div>";
            result += message;
            result += "</div>";
        }
        return result;
    }

    @SuppressWarnings("unused")
    private Map<String, String> getQueryStringValues(String querystring) {
        Map<String, String> queryvalues = new HashMap<String, String>();

        String[] splitvals = querystring.split("&");

        queryvalues.put("processorid", splitvals[0]);
        queryvalues.put(splitvals[1].split("=")[0], splitvals[1].split("=")[1]);
        queryvalues.put(splitvals[2].split("=")[0], splitvals[2].split("=")[1]);

        return queryvalues;
    }

}
