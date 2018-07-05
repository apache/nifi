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
package org.apache.nifi.processors.standard.util;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.ProxyHTTP;
import com.jcraft.jsch.ProxySOCKS5;
import com.jcraft.jsch.Session;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.nifi.processors.standard.util.FTPTransfer.createComponentProxyConfigSupplier;

public abstract class SSHTransfer implements FileTransfer {

    public static final PropertyDescriptor PRIVATE_KEY_PATH = new PropertyDescriptor.Builder()
        .name("Private Key Path")
        .description("The fully qualified path to the Private Key file")
        .required(false)
        .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();
    public static final PropertyDescriptor PRIVATE_KEY_PASSPHRASE = new PropertyDescriptor.Builder()
        .name("Private Key Passphrase")
        .description("Password for the private key")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .sensitive(true)
        .build();
    public static final PropertyDescriptor HOST_KEY_FILE = new PropertyDescriptor.Builder()
        .name("Host Key File")
        .description("If supplied, the given file will be used as the Host Key; otherwise, no use host key file will be used")
        .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
        .required(false)
        .build();
    public static final PropertyDescriptor STRICT_HOST_KEY_CHECKING = new PropertyDescriptor.Builder()
        .name("Strict Host Key Checking")
        .description("Indicates whether or not strict enforcement of hosts keys should be applied")
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
        .name("Port")
        .description("The port that the remote system is listening on for file transfers")
        .addValidator(StandardValidators.PORT_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .defaultValue("22")
        .build();
    public static final PropertyDescriptor USE_KEEPALIVE_ON_TIMEOUT = new PropertyDescriptor.Builder()
        .name("Send Keep Alive On Timeout")
        .description("Indicates whether or not to send a single Keep Alive message when SSH socket times out")
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
        .build();


    protected static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP_AUTH, ProxySpec.SOCKS_AUTH};
    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE
            = ProxyConfiguration.createProxyConfigPropertyDescriptor(true, PROXY_SPECS);

    protected final ComponentLog logger;
    protected final ProcessContext ctx;
    protected Channel channel;
    protected Session session;
    protected InputStream in;
    protected OutputStream out;

    protected boolean closed = false;

    public SSHTransfer(final ProcessContext processContext, final ComponentLog logger) {
        this.ctx = processContext;
        this.logger = logger;
    }

    public static void validateProxySpec(ValidationContext context, Collection<ValidationResult> results) {
        ProxyConfiguration.validateProxySpec(context, results, PROXY_SPECS);
    }

    public abstract String getSSHProtocolName();

    @Override
    public InputStream getInputStream(final String remoteFileName) throws IOException {
        return getInputStream(remoteFileName, null);
    }

    @Override
    public void flush() throws IOException {
        // nothing needed here
    }

    @Override
    public boolean flush(final FlowFile flowFile) throws IOException {
        return true;
    }

    protected boolean sessionIsValid(final FlowFile flowFile){
        if(session == null || !session.isConnected()) return false;

        //confirm we are connected to the correct server
        final String sessionhost = session.getHost();
        final int sessionport = session.getPort();

        final String desthost = ctx.getProperty(HOSTNAME).evaluateAttributeExpressions(flowFile).getValue();
        final int destport = ctx.getProperty(PORT).evaluateAttributeExpressions(flowFile).asInteger();

        return (sessionhost.equals(desthost) && sessionport == destport);
    }

    protected void establishSession(final FlowFile flowFile) throws IOException {
        if(sessionIsValid(flowFile)) return;

        //establish new SSH session
        this.close();
        this.session = getSession(ctx, flowFile);
        this.closed=false;
    }

    public static Session getSession(final ProcessContext processContext, final FlowFile flowFile) throws IOException {
        final JSch jsch = new JSch();
        try{
            final String username = processContext.getProperty(USERNAME).evaluateAttributeExpressions(flowFile).getValue();
            final Session session = jsch.getSession(username,
                    processContext.getProperty(HOSTNAME).evaluateAttributeExpressions(flowFile).getValue(),
                    processContext.getProperty(PORT).evaluateAttributeExpressions(flowFile).asInteger().intValue());

            final ProxyConfiguration proxyConfig = ProxyConfiguration.getConfiguration(processContext,
                    createComponentProxyConfigSupplier(processContext));
            switch (proxyConfig.getProxyType()) {
                case HTTP:
                    final ProxyHTTP proxyHTTP = new ProxyHTTP(proxyConfig.getProxyServerHost(), proxyConfig.getProxyServerPort());
                    // Check if Username is set and populate the proxy accordingly
                    if (proxyConfig.hasCredential()) {
                        proxyHTTP.setUserPasswd(proxyConfig.getProxyUserName(), proxyConfig.getProxyUserPassword());
                    }
                    session.setProxy(proxyHTTP);
                    break;
                case SOCKS:
                    final ProxySOCKS5 proxySOCKS5 = new ProxySOCKS5(proxyConfig.getProxyServerHost(), proxyConfig.getProxyServerPort());
                    if (proxyConfig.hasCredential()) {
                        proxySOCKS5.setUserPasswd(proxyConfig.getProxyUserName(), proxyConfig.getProxyUserPassword());
                    }
                    session.setProxy(proxySOCKS5);
                    break;

            }

            final String hostKeyVal = processContext.getProperty(HOST_KEY_FILE).getValue();
            if (hostKeyVal != null) {
                jsch.setKnownHosts(hostKeyVal);
            }

            final Properties properties = new Properties();
            properties.setProperty("StrictHostKeyChecking", processContext.getProperty(STRICT_HOST_KEY_CHECKING).asBoolean() ? "yes" : "no");
            properties.setProperty("PreferredAuthentications", "publickey,password,keyboard-interactive");

            final PropertyValue compressionValue = processContext.getProperty(FileTransfer.USE_COMPRESSION);
            if (compressionValue != null && "true".equalsIgnoreCase(compressionValue.getValue())) {
                properties.setProperty("compression.s2c", "zlib@openssh.com,zlib,none");
                properties.setProperty("compression.c2s", "zlib@openssh.com,zlib,none");
            } else {
                properties.setProperty("compression.s2c", "none");
                properties.setProperty("compression.c2s", "none");
            }

            session.setConfig(properties);

            final String privateKeyFile = processContext.getProperty(PRIVATE_KEY_PATH).evaluateAttributeExpressions(flowFile).getValue();
            if (privateKeyFile != null) {
                jsch.addIdentity(privateKeyFile, processContext.getProperty(PRIVATE_KEY_PASSPHRASE).evaluateAttributeExpressions(flowFile).getValue());
            }

            final String password = processContext.getProperty(FileTransfer.PASSWORD).evaluateAttributeExpressions(flowFile).getValue();
            if (password != null) {
                session.setPassword(password);
            }

            final int connectionTimeoutMillis = processContext.getProperty(FileTransfer.CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
            session.setTimeout(connectionTimeoutMillis);
            session.connect();

            return session;
        } catch (JSchException e) {
            throw new IOException(e);
        }
    }

    protected Channel getChannel(final FlowFile flowFile) throws IOException {
        return getChannel(flowFile, null);
    }

    protected Channel getChannel(final FlowFile flowFile, final PreConnectHandler preConnectHandler) throws IOException {
        if (sessionIsValid(flowFile) && channel != null && channel.isConnected()) {
            return channel;
        }

        try {
            if(!sessionIsValid(flowFile)){
                establishSession(flowFile);
            }

            channel = session.openChannel(this.getSSHProtocolName());

            in = channel.getInputStream();
            out = channel.getOutputStream();

            if(preConnectHandler != null){
                preConnectHandler.OnPreConnect(channel, flowFile);
            }

            final int connectionTimeoutMillis = ctx.getProperty(FileTransfer.CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
            channel.connect(connectionTimeoutMillis);
            session.setTimeout(ctx.getProperty(FileTransfer.DATA_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());
            if (!ctx.getProperty(USE_KEEPALIVE_ON_TIMEOUT).asBoolean()) {
                session.setServerAliveCountMax(0); // do not send keepalive message on SocketTimeoutException
            }

            return channel;
        } catch (JSchException e) {
            throw new IOException("Failed to obtain connection to remote host due to " + e.toString(), e);
        }
    }

    public interface PreConnectHandler{
        void OnPreConnect(Channel channel, FlowFile flowFile) throws IOException;
    }

    public void closeChannel(){
        try {
            if (null != channel) {
                channel.disconnect();
            }
        } catch (final Exception ex) {
            logger.warn("Failed to close Channel due to {}", new Object[] {ex.toString()}, ex);
        }
        channel = null;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        closeChannel();

        try {
            if (null != session) {
                session.disconnect();
            }
        } catch (final Exception ex) {
            logger.warn("Failed to close session due to {}", new Object[] {ex.toString()}, ex);
        }
        session = null;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    protected int numberPermissions(String perms) {
        int number = -1;
        final Pattern rwxPattern = Pattern.compile("^[rwx-]{9}$");
        final Pattern numPattern = Pattern.compile("\\d+");
        if (rwxPattern.matcher(perms).matches()) {
            number = 0;
            if (perms.charAt(0) == 'r') {
                number |= 0x100;
            }
            if (perms.charAt(1) == 'w') {
                number |= 0x80;
            }
            if (perms.charAt(2) == 'x') {
                number |= 0x40;
            }
            if (perms.charAt(3) == 'r') {
                number |= 0x20;
            }
            if (perms.charAt(4) == 'w') {
                number |= 0x10;
            }
            if (perms.charAt(5) == 'x') {
                number |= 0x8;
            }
            if (perms.charAt(6) == 'r') {
                number |= 0x4;
            }
            if (perms.charAt(7) == 'w') {
                number |= 0x2;
            }
            if (perms.charAt(8) == 'x') {
                number |= 0x1;
            }
        } else if (numPattern.matcher(perms).matches()) {
            try {
                number = Integer.parseInt(perms, 8);
            } catch (NumberFormatException ignore) {
            }
        }
        return number;
    }

    static {
        JSch.setLogger(new com.jcraft.jsch.Logger() {
            @Override
            public boolean isEnabled(int level) {
                return true;
            }

            @Override
            public void log(int level, String message) {
                LoggerFactory.getLogger(SSHTransfer.class).debug("JSch Log: {}", message);
            }
        });
    }
}
