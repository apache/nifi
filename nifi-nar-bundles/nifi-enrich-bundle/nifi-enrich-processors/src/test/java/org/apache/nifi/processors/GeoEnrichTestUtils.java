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
package org.apache.nifi.processors;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.maxmind.geoip2.model.CityResponse;

import java.util.Collections;

public class GeoEnrichTestUtils {
    public static CityResponse getFullCityResponse() throws Exception {
        // Taken from MaxMind unit tests.
        final String maxMindCityResponse = "{\"city\":{\"confidence\":76,"
                + "\"geoname_id\":9876,\"names\":{\"en\":\"Minneapolis\""
                + "}},\"continent\":{\"code\":\"NA\","
                + "\"geoname_id\":42,\"names\":{" + "\"en\":\"North America\""
                + "}},\"country\":{\"confidence\":99,"
                + "\"iso_code\":\"US\",\"geoname_id\":1,\"names\":{"
                + "\"en\":\"United States of America\"" + "}" + "},"
                + "\"location\":{" + "\"accuracy_radius\":1500,"
                + "\"latitude\":44.98," + "\"longitude\":93.2636,"
                + "\"metro_code\":765," + "\"time_zone\":\"America/Chicago\""
                + "}," + "\"postal\":{\"confidence\": 33, \"code\":\"55401\"},"
                + "\"registered_country\":{" + "\"geoname_id\":2,"
                + "\"iso_code\":\"CA\"," + "\"names\":{" + "\"en\":\"Canada\""
                + "}" + "}," + "\"represented_country\":{" + "\"geoname_id\":3,"
                + "\"iso_code\":\"GB\"," + "\"names\":{"
                + "\"en\":\"United Kingdom\"" + "}," + "\"type\":\"C<military>\""
                + "}," + "\"subdivisions\":[{" + "\"confidence\":88,"
                + "\"geoname_id\":574635," + "\"iso_code\":\"MN\"," + "\"names\":{"
                + "\"en\":\"Minnesota\"" + "}" + "}," + "{\"iso_code\":\"TT\"}],"
                + "\"traits\":{" + "\"autonomous_system_number\":1234,"
                + "\"autonomous_system_organization\":\"AS Organization\","
                + "\"domain\":\"example.com\"," + "\"ip_address\":\"1.2.3.4\","
                + "\"is_anonymous_proxy\":true,"
                + "\"is_satellite_provider\":true," + "\"isp\":\"Comcast\","
                + "\"organization\":\"Blorg\"," + "\"user_type\":\"college\""
                + "}," + "\"maxmind\":{\"queries_remaining\":11}" + "}";

        InjectableValues inject = new InjectableValues.Std().addValue("locales", Collections.singletonList("en"));
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        return new ObjectMapper().reader(CityResponse.class).with(inject).readValue(maxMindCityResponse);
    }

    public static CityResponse getNullLatAndLongCityResponse() throws Exception {
        // Taken from MaxMind unit tests and modified.
        final String maxMindCityResponse = "{" + "\"city\":{" + "\"confidence\":76,"
                + "\"geoname_id\":9876," + "\"names\":{" + "\"en\":\"Minneapolis\""
                + "}" + "}," + "\"continent\":{" + "\"code\":\"NA\","
                + "\"geoname_id\":42," + "\"names\":{" + "\"en\":\"North America\""
                + "}" + "}," + "\"country\":{" + "\"confidence\":99,"
                + "\"iso_code\":\"US\"," + "\"geoname_id\":1," + "\"names\":{"
                + "\"en\":\"United States of America\"" + "}" + "},"
                + "\"location\":{" + "\"accuracy_radius\":1500,"
                + "\"metro_code\":765," + "\"time_zone\":\"America/Chicago\""
                + "}," + "\"postal\":{\"confidence\": 33, \"code\":\"55401\"},"
                + "\"registered_country\":{" + "\"geoname_id\":2,"
                + "\"iso_code\":\"CA\"," + "\"names\":{" + "\"en\":\"Canada\""
                + "}" + "}," + "\"represented_country\":{" + "\"geoname_id\":3,"
                + "\"iso_code\":\"GB\"," + "\"names\":{"
                + "\"en\":\"United Kingdom\"" + "}," + "\"type\":\"C<military>\""
                + "}," + "\"subdivisions\":[{" + "\"confidence\":88,"
                + "\"geoname_id\":574635," + "\"iso_code\":\"MN\"," + "\"names\":{"
                + "\"en\":\"Minnesota\"" + "}" + "}," + "{\"iso_code\":\"TT\"}],"
                + "\"traits\":{" + "\"autonomous_system_number\":1234,"
                + "\"autonomous_system_organization\":\"AS Organization\","
                + "\"domain\":\"example.com\"," + "\"ip_address\":\"1.2.3.4\","
                + "\"is_anonymous_proxy\":true,"
                + "\"is_satellite_provider\":true," + "\"isp\":\"Comcast\","
                + "\"organization\":\"Blorg\"," + "\"user_type\":\"college\""
                + "}," + "\"maxmind\":{\"queries_remaining\":11}" + "}";

        InjectableValues inject = new InjectableValues.Std().addValue("locales", Collections.singletonList("en"));
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        return new ObjectMapper().reader(CityResponse.class).with(inject).readValue(maxMindCityResponse);
    }
}
