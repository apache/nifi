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
package org.apache.nifi.lookup.maxmind;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.maxmind.db.Metadata;
import com.maxmind.db.Reader;
import com.maxmind.db.Reader.FileMode;
import com.maxmind.geoip2.GeoIp2Provider;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.AnonymousIpResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.ConnectionTypeResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.model.DomainResponse;
import com.maxmind.geoip2.model.IspResponse;

/**
 * <p>
 * This class was copied from https://raw.githubusercontent.com/maxmind/GeoIP2-java/master/src/main/java/com/maxmind/geoip2/DatabaseReader.java It is written by Maxmind and it is available under
 * Apache Software License V2
 *
 * The modification we're making to the code below is to stop using exceptions for mainline flow control. Specifically we don't want to throw an exception simply because an address was not found.
 * </p>
 *
 * Instances of this class provide a reader for the GeoIP2 database format. IP addresses can be looked up using the <code>get</code> method.
 */
public class DatabaseReader implements GeoIp2Provider, Closeable {

    private final Reader reader;
    private final ObjectMapper om;

    private DatabaseReader(Builder builder) throws IOException {
        if (builder.stream != null) {
            this.reader = new Reader(builder.stream);
        } else if (builder.database != null) {
            this.reader = new Reader(builder.database, builder.mode);
        } else {
            // This should never happen. If it does, review the Builder class
            // constructors for errors.
            throw new IllegalArgumentException("Unsupported Builder configuration: expected either File or URL");
        }

        this.om = new ObjectMapper();
        this.om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.om.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        InjectableValues inject = new InjectableValues.Std().addValue("locales", builder.locales);
        this.om.setInjectableValues(inject);
    }

    /**
     * <p>
     * Constructs a Builder for the DatabaseReader. The file passed to it must be a valid GeoIP2 database file.
     * </p>
     * <p>
     * <code>Builder</code> creates instances of <code>DatabaseReader</code> from values set by the methods.
     * </p>
     * <p>
     * Only the values set in the <code>Builder</code> constructor are required.
     * </p>
     */
    public final static class Builder {

        final File database;
        final InputStream stream;

        List<String> locales = Arrays.asList("en");
        FileMode mode = FileMode.MEMORY_MAPPED;

        /**
         * @param stream the stream containing the GeoIP2 database to use.
         */
        public Builder(InputStream stream) {
            this.stream = stream;
            this.database = null;
        }

        /**
         * @param database the GeoIP2 database file to use.
         */
        public Builder(File database) {
            this.database = database;
            this.stream = null;
        }

        /**
         * @param val List of locale codes to use in name property from most preferred to least preferred.
         * @return Builder object
         */
        public Builder locales(List<String> val) {
            this.locales = val;
            return this;
        }

        /**
         * @param val The file mode used to open the GeoIP2 database
         * @return Builder object
         * @throws java.lang.IllegalArgumentException if you initialized the Builder with a URL, which uses {@link FileMode#MEMORY}, but you provided a different FileMode to this method.
         */
        public Builder fileMode(FileMode val) {
            if (this.stream != null && !FileMode.MEMORY.equals(val)) {
                throw new IllegalArgumentException("Only FileMode.MEMORY is supported when using an InputStream.");
            }

            this.mode = val;
            return this;
        }

        /**
         * @return an instance of <code>DatabaseReader</code> created from the fields set on this builder.
         * @throws IOException if there is an error reading the database
         */
        public DatabaseReader build() throws IOException {
            return new DatabaseReader(this);
        }
    }

    /**
     * @param ipAddress IPv4 or IPv6 address to lookup.
     * @return An object of type T with the data for the IP address or null if no information could be found for the given IP address
     * @throws IOException if there is an error opening or reading from the file.
     */
    private <T> T get(InetAddress ipAddress, Class<T> cls, boolean hasTraits, String type) throws IOException, AddressNotFoundException {
        ObjectNode node = (ObjectNode) this.reader.get(ipAddress);
        if (node == null) {
            return null;
        }

        ObjectNode ipNode;
        if (hasTraits) {
            if (!node.has("traits")) {
                node.set("traits", this.om.createObjectNode());
            }

            ipNode = (ObjectNode) node.get("traits");
        } else {
            ipNode = node;
        }

        ipNode.put("ip_address", ipAddress.getHostAddress());
        return this.om.treeToValue(node, cls);
    }

    /**
     * <p>
     * Closes the database.
     * </p>
     * <p>
     * If you are using <code>FileMode.MEMORY_MAPPED</code>, this will
     * <em>not</em> unmap the underlying file due to a limitation in Java's <code>MappedByteBuffer</code>. It will however set the reference to the buffer to <code>null</code>, allowing the garbage
     * collector to collect it.
     * </p>
     *
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public void close() throws IOException {
        this.reader.close();
    }

    @Override
    public CountryResponse country(InetAddress ipAddress) throws IOException, GeoIp2Exception {
        return this.get(ipAddress, CountryResponse.class, true, "Country");
    }

    @Override
    public CityResponse city(InetAddress ipAddress) throws IOException, GeoIp2Exception {
        return this.get(ipAddress, CityResponse.class, true, "City");
    }

    /**
     * Look up an IP address in a GeoIP2 Anonymous IP.
     *
     * @param ipAddress IPv4 or IPv6 address to lookup.
     * @return a AnonymousIpResponse for the requested IP address.
     * @throws GeoIp2Exception if there is an error looking up the IP
     * @throws IOException if there is an IO error
     */
    public AnonymousIpResponse anonymousIp(InetAddress ipAddress) throws IOException, GeoIp2Exception {
        return this.get(ipAddress, AnonymousIpResponse.class, false, "GeoIP2-Anonymous-IP");
    }

    /**
     * Look up an IP address in a GeoIP2 Connection Type database.
     *
     * @param ipAddress IPv4 or IPv6 address to lookup.
     * @return a ConnectTypeResponse for the requested IP address.
     * @throws GeoIp2Exception if there is an error looking up the IP
     * @throws IOException if there is an IO error
     */
    public ConnectionTypeResponse connectionType(InetAddress ipAddress) throws IOException, GeoIp2Exception {
        return this.get(ipAddress, ConnectionTypeResponse.class, false,
            "GeoIP2-Connection-Type");
    }

    /**
     * Look up an IP address in a GeoIP2 Domain database.
     *
     * @param ipAddress IPv4 or IPv6 address to lookup.
     * @return a DomainResponse for the requested IP address.
     * @throws GeoIp2Exception if there is an error looking up the IP
     * @throws IOException if there is an IO error
     */
    public DomainResponse domain(InetAddress ipAddress) throws IOException, GeoIp2Exception {
        return this.get(ipAddress, DomainResponse.class, false, "GeoIP2-Domain");
    }

    /**
     * Look up an IP address in a GeoIP2 ISP database.
     *
     * @param ipAddress IPv4 or IPv6 address to lookup.
     * @return an IspResponse for the requested IP address.
     * @throws GeoIp2Exception if there is an error looking up the IP
     * @throws IOException if there is an IO error
     */
    public IspResponse isp(InetAddress ipAddress) throws IOException, GeoIp2Exception {
        return this.get(ipAddress, IspResponse.class, false, "GeoIP2-ISP");
    }

    /**
     * @return the metadata for the open MaxMind DB file.
     */
    public Metadata getMetadata() {
        return this.reader.getMetadata();
    }
}
