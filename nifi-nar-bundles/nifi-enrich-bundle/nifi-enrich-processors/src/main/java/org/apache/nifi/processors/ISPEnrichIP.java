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

import com.maxmind.geoip2.model.IspResponse;
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
@Tags({"ISP", "enrich", "ip", "maxmind"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Looks up ISP information for an IP address and adds the information to FlowFile attributes. The "
        + "ISP data is provided as a MaxMind ISP database (Note that this is NOT the same as the GeoLite database utilized" +
        "by some geo enrichment tools). The attribute that contains the IP address to lookup is provided by the " +
        "'IP Address Attribute' property. If the name of the attribute provided is 'X', then the the attributes added by" +
        " enrichment will take the form X.isp.<fieldName>")
@WritesAttributes({
    @WritesAttribute(attribute = "X.isp.lookup.micros", description = "The number of microseconds that the geo lookup took"),
    @WritesAttribute(attribute = "X.isp.asn", description = "The Autonomous System Number (ASN) identified for the IP address"),
    @WritesAttribute(attribute = "X.isp.asn.organization", description = "The Organization Associated with the ASN identified"),
    @WritesAttribute(attribute = "X.isp.name", description = "The name of the ISP associated with the IP address provided"),
    @WritesAttribute(attribute = "X.isp.organization", description = "The Organization associated with the IP address provided"),})
public class ISPEnrichIP extends AbstractEnrichIP {

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
        IspResponse response = null;

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
            response = dbReader.isp(inetAddress);
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
        attrs.put(new StringBuilder(ipAttributeName).append(".isp.lookup.micros").toString(), String.valueOf(stopWatch.getDuration(TimeUnit.MICROSECONDS)));



        // During test I observed behavior where null values in ASN data could trigger NPEs. Instead of relying on the
        // underlying database to be free from Nulls wrapping ensure equality to null without assigning a variable
        // seem like good option to "final int asn ..." as with the other returned data.
        if (!(response.getAutonomousSystemNumber() == null)) {
            attrs.put(new StringBuilder(ipAttributeName).append(".isp.asn").toString(), String.valueOf(response.getAutonomousSystemNumber()));
        }
        final String asnOrg = response.getAutonomousSystemOrganization();
        if (asnOrg != null) {
            attrs.put(new StringBuilder(ipAttributeName).append(".isp.asn.organization").toString(), asnOrg);
        }

        final String ispName = response.getIsp();
        if (ispName != null) {
            attrs.put(new StringBuilder(ipAttributeName).append(".isp.name").toString(), ispName);
        }

        final String organisation = response.getOrganization();
        if (organisation  != null) {
            attrs.put(new StringBuilder(ipAttributeName).append(".isp.organization").toString(), organisation);
        }

        flowFile = session.putAllAttributes(flowFile, attrs);

        session.transfer(flowFile, REL_FOUND);
    }

}
