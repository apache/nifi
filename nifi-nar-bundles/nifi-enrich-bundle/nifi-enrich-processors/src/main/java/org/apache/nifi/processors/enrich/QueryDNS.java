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

package org.apache.nifi.processors.enrich;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.naming.Context;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import org.apache.commons.lang3.StringUtils;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;


@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"dns", "enrich", "ip"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("A powerful DNS query processor primary designed to enrich DataFlows with DNS based APIs " +
        "(e.g. RBLs, ShadowServer's ASN lookup) but that can be also used to perform regular DNS lookups.")
@WritesAttributes({
        @WritesAttribute(attribute = "enrich.dns.record*.group*", description = "The captured fields of the DNS query response for each of the records received"),
})
public class QueryDNS extends AbstractEnrichProcessor {

    public static final PropertyDescriptor DNS_QUERY_TYPE = new PropertyDescriptor.Builder()
            .name("DNS_QUERY_TYPE")
            .displayName("DNS Query Type")
            .description("The DNS query type to be used by the processor (e.g. TXT, A)")
            .required(true)
            .defaultValue("TXT")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DNS_SERVER = new PropertyDescriptor.Builder()
            .name("DNS_SERVER")
            .displayName("DNS Servers")
            .description("A comma separated list of  DNS servers to be used. (Defaults to system wide if none is used)")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DNS_TIMEOUT = new PropertyDescriptor.Builder()
            .name("DNS_TIMEOUT")
            .displayName("DNS Query Timeout")
            .description("The amount of time to wait until considering a query as failed")
            .required(true)
            .defaultValue("1500 ms")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor DNS_RETRIES = new PropertyDescriptor.Builder()
            .name("DNS_RETRIES")
            .displayName("DNS Query Retries")
            .description("The number of attempts before giving up and moving on")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    private DirContext ictx;

    // Assign the default and generally used contextFactory value
    private String contextFactory = com.sun.jndi.dns.DnsContextFactory.class.getName();;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(QUERY_INPUT);
        props.add(QUERY_PARSER);
        props.add(QUERY_PARSER_INPUT);
        props.add(DNS_RETRIES);
        props.add(DNS_TIMEOUT);
        props.add(DNS_SERVER);
        props.add(DNS_QUERY_TYPE);
        propertyDescriptors = Collections.unmodifiableList(props);

        Set<Relationship> rels = new HashSet<>();
        rels.add(REL_FOUND);
        rels.add(REL_NOT_FOUND);
        relationships = Collections.unmodifiableSet(rels);
    }
    private AtomicBoolean initialized = new AtomicBoolean(false);


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }


    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        if (!initialized.get()) {
            initializeResolver(context);
            getLogger().warn("Resolver was initialized at onTrigger instead of onScheduled");

        }

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String queryType = context.getProperty(DNS_QUERY_TYPE).getValue();
        final String queryInput = context.getProperty(QUERY_INPUT).evaluateAttributeExpressions(flowFile).getValue();
        final String queryParser = context.getProperty(QUERY_PARSER).getValue();
        final String queryRegex = context.getProperty(QUERY_PARSER_INPUT).getValue();

        boolean found = false;
        try {
            Attributes results = doLookup(queryInput, queryType);
            // NOERROR & NODATA seem to return empty Attributes handled bellow
            // but defaulting to not found in any case
            if (results.size() < 1) {
                found = false;
            } else {
                int recordNumber = 0;
                NamingEnumeration<?> dnsEntryIterator = results.get(queryType).getAll();

                while (dnsEntryIterator.hasMoreElements()) {
                    String dnsRecord = dnsEntryIterator.next().toString();
                    // While NXDOMAIN is being generated by doLookup catch

                    if (dnsRecord != "NXDOMAIN") {
                        Map<String, String> parsedResults = parseResponse(recordNumber, dnsRecord, queryParser, queryRegex, "dns");
                        flowFile = session.putAllAttributes(flowFile, parsedResults);
                        found = true;
                    } else {
                        // Otherwise treat as not found
                        found = false;
                    }

                    // Increase the counter and iterate over next record....
                    recordNumber++;
                }
            }
        } catch (NamingException e) {
            context.yield();
            throw new ProcessException("Unexpected NamingException while processing records. Please review your configuration.", e);

        }

        // Finally prepare to send the data down the pipeline
        if (found) {
            // Sending the resulting flowfile (with attributes) to REL_FOUND
            session.transfer(flowFile, REL_FOUND);
        } else {
            // NXDOMAIN received, accepting the fate but forwarding
            // to REL_NOT_FOUND
            session.transfer(flowFile, REL_NOT_FOUND);
        }
    }


    @OnScheduled
    public void onScheduled(ProcessContext context) {
        try {
            initializeResolver(context);
        } catch (Exception e) {
            context.yield();
            throw new ProcessException("Failed to initialize the JNDI DNS resolver server", e);
        }
    }


    protected void initializeResolver(final ProcessContext context ) {

        final String dnsTimeout = context.getProperty(DNS_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).toString();
        final String dnsServer = context.getProperty(DNS_SERVER).getValue();
        final String dnsRetries = context.getProperty(DNS_RETRIES).getValue();


        String finalServer = "";
        Hashtable<String,String> env = new Hashtable<String,String>();
        env.put("java.naming.factory.initial", contextFactory);
        env.put("com.sun.jndi.dns.timeout.initial", dnsTimeout);
        env.put("com.sun.jndi.dns.timeout.retries", dnsRetries);
        if (StringUtils.isNotEmpty(dnsServer)) {
            for (String server : dnsServer.split(",")) {
                finalServer = finalServer + "dns://" + server + "/. ";
            }
            env.put(Context.PROVIDER_URL, finalServer);
        }

        try {
            initializeContext(env);
            initialized.set(true);
        } catch (NamingException e) {
            getLogger().error("Could not initialize JNDI context", e);
        }
    }


    /**
     * This method performs a simple DNS lookup using JNDI
     * @param queryInput String containing the query body itself (e.g. 4.3.3.1.in-addr.arpa);
     * @param queryType String containing the query type (e.g. TXT);
     */
    protected Attributes doLookup(String queryInput, String queryType) throws NamingException {
        // This is a simple DNS lookup attempt

        Attributes attrs;

        try {
            // Uses pre-existing context to resolve
            attrs = ictx.getAttributes(queryInput, new String[]{queryType});
            return attrs;
        } catch ( NameNotFoundException e) {
            getLogger().debug("Resolution for domain {} failed due to {}", new Object[]{queryInput, e});
            attrs = new BasicAttributes(queryType, "NXDOMAIN",true);
            return attrs;
        }
    }

    // This was separated from main code to ease the creation of test units injecting fake JNDI data
    // back into the processor.
    protected void initializeContext(Hashtable<String,String> env) throws NamingException {
        this.ictx = new InitialDirContext(env);
        this.initialized =  new AtomicBoolean(false);
        initialized.set(true);
    }

}
