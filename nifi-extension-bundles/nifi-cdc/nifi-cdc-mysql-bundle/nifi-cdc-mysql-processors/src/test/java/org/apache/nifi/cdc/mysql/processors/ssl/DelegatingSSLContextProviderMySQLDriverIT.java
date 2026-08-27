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
package org.apache.nifi.cdc.mysql.processors.ssl;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DelegatingSSLContextProviderMySQLDriverIT {

    private static final String MYSQL_IMAGE = "mysql:8.4";

    private static final String SSL_CONTEXT_PROTOCOL = "TLS";

    private static final String MYSQL_CA_PATH = "/var/lib/mysql/ca.pem";

    private static final String CERTIFICATE_TYPE = "X.509";

    private static final String SSL_CIPHER_STATUS_QUERY = "SHOW SESSION STATUS LIKE 'Ssl_cipher'";

    private static MySQLContainer mysql;

    @BeforeAll
    static void startContainer() {
        mysql = new MySQLContainer(DockerImageName.parse(MYSQL_IMAGE));
        mysql.start();
    }

    @AfterAll
    static void stopContainer() {
        if (mysql != null) {
            mysql.stop();
        }
    }

    @Test
    void testRequiredModeUsesInjectedSslContext() throws Exception {
        final AtomicBoolean trustManagerInvoked = new AtomicBoolean(false);
        final SSLContext sslContext = SSLContext.getInstance(SSL_CONTEXT_PROTOCOL);
        sslContext.init(null, new TrustManager[]{new RecordingTrustManager(trustManagerInvoked)}, new SecureRandom());

        try (Connection connection = connect(SSLMode.REQUIRED, sslContext)) {
            assertTlsActive(connection);
        }

        assertTrue(trustManagerInvoked.get());
    }

    @Test
    void testVerifyCaSucceedsWhenInjectedContextTrustsServerAuthority() throws Exception {
        final SSLContext sslContext = buildContextTrusting(readServerCa());

        try (Connection connection = connect(SSLMode.VERIFY_CA, sslContext)) {
            assertTlsActive(connection);
        }
    }

    @Test
    void testVerifyCaFailsWhenInjectedContextDoesNotTrustServerAuthority() throws Exception {
        final SSLContext sslContext = buildContextTrusting(null);

        assertThrows(SQLException.class, () -> connect(SSLMode.VERIFY_CA, sslContext));
    }

    private Connection connect(final SSLMode sslMode, final SSLContext sslContext) throws SQLException {
        final Map<String, String> sslProperties = new StandardConnectionPropertiesProvider(sslMode).getConnectionProperties();

        final Properties properties = new Properties();
        properties.putAll(sslProperties);
        properties.setProperty("user", mysql.getUsername());
        properties.setProperty("password", mysql.getPassword());

        final String providerName = DelegatingSSLContextProvider.class.getSimpleName() + UUID.randomUUID();
        properties.setProperty(SecurityProperty.SSL_CONTEXT_PROVIDER.getProperty(), providerName);
        final Provider provider = new DelegatingSSLContextProvider(providerName, sslContext);
        Security.addProvider(provider);
        try {
            return DriverManager.getConnection(mysql.getJdbcUrl(), properties);
        } finally {
            Security.removeProvider(providerName);
        }
    }

    private void assertTlsActive(final Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SSL_CIPHER_STATUS_QUERY)) {
            assertTrue(resultSet.next());
            final String cipher = resultSet.getString(2);
            assertNotNull(cipher);
            assertFalse(cipher.isEmpty(), "Connection is not using TLS");
        }
    }

    private X509Certificate readServerCa() throws Exception {
        final byte[] caBytes = mysql.copyFileFromContainer(MYSQL_CA_PATH, InputStream::readAllBytes);
        final CertificateFactory certificateFactory = CertificateFactory.getInstance(CERTIFICATE_TYPE);
        return (X509Certificate) certificateFactory.generateCertificate(new ByteArrayInputStream(caBytes));
    }

    private SSLContext buildContextTrusting(final X509Certificate trustedCertificate) throws Exception {
        final KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        if (trustedCertificate != null) {
            trustStore.setCertificateEntry("mysql-ca", trustedCertificate);
        }

        final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        final SSLContext sslContext = SSLContext.getInstance(SSL_CONTEXT_PROTOCOL);
        sslContext.init(null, trustManagerFactory.getTrustManagers(), new SecureRandom());
        return sslContext;
    }

    private record RecordingTrustManager(AtomicBoolean invoked) implements X509TrustManager {

        @Override
        public void checkClientTrusted(final X509Certificate[] chain, final String authType) {
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] chain, final String authType) {
            invoked.set(true);
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
