package org.apache.nifi.cypher;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.tinkerpop.gremlin.driver.Cluster;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public abstract class AbstractTinkerpopClientService extends AbstractControllerService {
    public static final PropertyDescriptor CONTACT_POINTS = new PropertyDescriptor.Builder()
            .name("opencypher-contact-points")
            .displayName("Contact Points")
            .description("A comma-separated list of hostnames or IP addresses where an OpenCypher-enabled server can be found.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                    + "connections.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
            CONTACT_POINTS, SSL_CONTEXT_SERVICE
    ));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    protected Cluster.Builder setupSSL(ConfigurationContext context, Cluster.Builder builder) {
        if (context.getProperty(SSL_CONTEXT_SERVICE).isSet()) {
            SSLContextService service = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
            builder
                    .enableSsl(true)
                    .keyStore(service.getKeyStoreFile())
                    .keyStorePassword(service.getKeyStorePassword())
                    .keyStoreType(service.getKeyStoreType())
                    .trustStore(service.getTrustStoreFile())
                    .trustStorePassword(service.getTrustStorePassword());
        }

        return builder;
    }

    protected Cluster buildCluster(ConfigurationContext context) {
        String contactProp = context.getProperty(CONTACT_POINTS).getValue();
        String[] contactPoints = contactProp.split(",[\\s]*");
        Cluster.Builder builder = Cluster.build();
        for (String contactPoint : contactPoints) {
            builder.addContactPoint(contactPoint.trim());
        }

        builder = setupSSL(context, builder);

        return builder.create();
    }
}
