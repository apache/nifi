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

import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.Subdivision;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.maxmind.DatabaseReader;
import org.apache.nifi.util.StopWatch;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"geo", "enrich", "ip", "maxmind"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Looks up geolocation information for an IP address and adds the geo information to FlowFile attributes. The "
        + "geo data is provided as a MaxMind database. The attribute that contains the IP address to lookup is provided by the "
        + "'IP Address Attribute' property. If the name of the attribute provided is 'X', then the the attributes added by enrichment "
        + "will take the form X.geo.<fieldName>")
@WritesAttributes({
    @WritesAttribute(attribute = "X.geo.lookup.micros", description = "The number of microseconds that the geo lookup took"),
    @WritesAttribute(attribute = "X.geo.city", description = "The city identified for the IP address"),
    @WritesAttribute(attribute = "X.geo.accuracy", description = "The accuracy radius if provided by the database (in Kilometers)"),
    @WritesAttribute(attribute = "X.geo.latitude", description = "The latitude identified for this IP address"),
    @WritesAttribute(attribute = "X.geo.longitude", description = "The longitude identified for this IP address"),
    @WritesAttribute(attribute = "X.geo.subdivision.N",
            description = "Each subdivision that is identified for this IP address is added with a one-up number appended to the attribute name, starting with 0"),
    @WritesAttribute(attribute = "X.geo.subdivision.isocode.N", description = "The ISO code for the subdivision that is identified by X.geo.subdivision.N"),
    @WritesAttribute(attribute = "X.geo.country", description = "The country identified for this IP address"),
    @WritesAttribute(attribute = "X.geo.country.isocode", description = "The ISO Code for the country identified"),
    @WritesAttribute(attribute = "X.geo.postalcode", description = "The postal code for the country identified"),})
public class GeoEnrichIP extends AbstractEnrichIP {

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final DatabaseReader dbReader = databaseReaderRef.get();
        final String ipAttributeName = context.getProperty(IP_ADDRESS_ATTRIBUTE).evaluateAttributeExpressions(flowFile).getValue();
        final String ipAttributeValue = flowFile.getAttribute(ipAttributeName);

        if (StringUtils.isEmpty(ipAttributeName)) {
            session.transfer(flowFile, REL_NOT_FOUND);
            getLogger().warn("FlowFile '{}' attribute '{}' was empty. Routing to failure",
                    new Object[]{flowFile, IP_ADDRESS_ATTRIBUTE.getDisplayName()});
            return;
        }

        InetAddress inetAddress = null;
        CityResponse response = null;

        try {
            inetAddress = InetAddress.getByName(ipAttributeValue);
        } catch (final IOException ioe) {
            session.transfer(flowFile, REL_NOT_FOUND);
            getLogger().warn("Could not resolve the IP for value '{}', contained within the attribute '{}' in " +
                            "FlowFile '{}'. This is usually caused by issue resolving the appropriate DNS record or " +
                            "providing the processor with an invalid IP address ",
                            new Object[]{ipAttributeValue, IP_ADDRESS_ATTRIBUTE.getDisplayName(), flowFile}, ioe);
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);
        try {
            response = dbReader.city(inetAddress);
            stopWatch.stop();
        } catch (final IOException ex) {
            // Note IOException is captured again as dbReader also makes InetAddress.getByName() calls.
            // Most name or IP resolutions failure should have been triggered in the try loop above but
            // environmental conditions may trigger errors during the second resolution as well.
            session.transfer(flowFile, REL_NOT_FOUND);
            getLogger().warn("Failure while trying to find enrichment data for {} due to {}", new Object[]{flowFile, ex}, ex);
            return;
        }

        if (response == null) {
            session.transfer(flowFile, REL_NOT_FOUND);
            return;
        }

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(new StringBuilder(ipAttributeName).append(".geo.lookup.micros").toString(), String.valueOf(stopWatch.getDuration(TimeUnit.MICROSECONDS)));
        attrs.put(new StringBuilder(ipAttributeName).append(".geo.city").toString(), response.getCity().getName());

        final Double latitude = response.getLocation().getLatitude();
        if (latitude != null) {
            attrs.put(new StringBuilder(ipAttributeName).append(".geo.latitude").toString(), latitude.toString());
        }

        final Double longitude = response.getLocation().getLongitude();
        if (longitude != null) {
            attrs.put(new StringBuilder(ipAttributeName).append(".geo.longitude").toString(), longitude.toString());
        }

        final Integer accuracy = response.getLocation().getAccuracyRadius();
        if (accuracy != null) {
            attrs.put(new StringBuilder(ipAttributeName).append(".accuracy").toString(), String.valueOf(accuracy));
        }

        int i = 0;
        for (final Subdivision subd : response.getSubdivisions()) {
            attrs.put(new StringBuilder(ipAttributeName).append(".geo.subdivision.").append(i).toString(), subd.getName());
            attrs.put(new StringBuilder(ipAttributeName).append(".geo.subdivision.isocode.").append(i).toString(), subd.getIsoCode());
            i++;
        }
        attrs.put(new StringBuilder(ipAttributeName).append(".geo.country").toString(), response.getCountry().getName());
        attrs.put(new StringBuilder(ipAttributeName).append(".geo.country.isocode").toString(), response.getCountry().getIsoCode());
        attrs.put(new StringBuilder(ipAttributeName).append(".geo.postalcode").toString(), response.getPostal().getCode());
        flowFile = session.putAllAttributes(flowFile, attrs);

        session.transfer(flowFile, REL_FOUND);
    }

}
