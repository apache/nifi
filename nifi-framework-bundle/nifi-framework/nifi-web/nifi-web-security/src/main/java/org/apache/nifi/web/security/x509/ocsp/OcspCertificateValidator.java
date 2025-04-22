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
package org.apache.nifi.web.security.x509.ocsp;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.URI;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.Response;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.deprecation.log.DeprecationLogger;
import org.apache.nifi.deprecation.log.DeprecationLoggerFactory;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.x509.ocsp.OcspStatus.ValidationStatus;
import org.apache.nifi.web.security.x509.ocsp.OcspStatus.VerificationStatus;
import org.apache.nifi.web.util.WebClientUtils;
import org.bouncycastle.asn1.DEROctetString;
import org.bouncycastle.asn1.ocsp.OCSPObjectIdentifiers;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.Extensions;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.ocsp.BasicOCSPResp;
import org.bouncycastle.cert.ocsp.CertificateID;
import org.bouncycastle.cert.ocsp.CertificateStatus;
import org.bouncycastle.cert.ocsp.OCSPException;
import org.bouncycastle.cert.ocsp.OCSPReq;
import org.bouncycastle.cert.ocsp.OCSPReqBuilder;
import org.bouncycastle.cert.ocsp.OCSPResp;
import org.bouncycastle.cert.ocsp.OCSPRespBuilder;
import org.bouncycastle.cert.ocsp.RevokedStatus;
import org.bouncycastle.cert.ocsp.SingleResp;
import org.bouncycastle.operator.DigestCalculatorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentVerifierProviderBuilder;
import org.bouncycastle.operator.jcajce.JcaDigestCalculatorProviderBuilder;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OcspCertificateValidator {

    private static final Logger logger = LoggerFactory.getLogger(OcspCertificateValidator.class);

    private static final DeprecationLogger deprecationLogger = DeprecationLoggerFactory.getLogger(OcspCertificateValidator.class);

    private static final String OCSP_REQUEST_CONTENT_TYPE = "application/ocsp-request";

    private static final int CONNECT_TIMEOUT = 10000;
    private static final int READ_TIMEOUT = 10000;

    private URI validationAuthorityURI;
    private Client client;
    private Map<String, X509Certificate> trustedCAs;

    private LoadingCache<OcspRequest, OcspStatus> ocspCache;

    public OcspCertificateValidator(final NiFiProperties properties) {
        // get the responder url
        final String rawValidationAuthorityUrl = properties.getProperty(NiFiProperties.SECURITY_OCSP_RESPONDER_URL);

        // set properties when appropriate
        if (StringUtils.isNotBlank(rawValidationAuthorityUrl)) {
            deprecationLogger.warn("OCSP Certificate Validation with Responder URL [{}] is deprecated for removal", NiFiProperties.SECURITY_OCSP_RESPONDER_URL);

            try {
                // attempt to parse the specified va url
                validationAuthorityURI = URI.create(rawValidationAuthorityUrl);

                final ClientConfig clientConfig = new ClientConfig();
                clientConfig.property(ClientProperties.READ_TIMEOUT, READ_TIMEOUT);
                clientConfig.property(ClientProperties.CONNECT_TIMEOUT, CONNECT_TIMEOUT);

                // initialize the client
                client = WebClientUtils.createClient(clientConfig);

                // get the trusted CAs
                trustedCAs = getTrustedCAs(properties);

                // consider the ocsp certificate is specified
                final X509Certificate ocspCertificate = getOcspCertificate(properties);
                if (ocspCertificate != null) {
                    trustedCAs.put(ocspCertificate.getSubjectX500Principal().getName(), ocspCertificate);
                }

                // TODO - determine how long to cache the ocsp responses for
                final long cacheDurationMillis = FormatUtils.getTimeDuration("12 hours", TimeUnit.MILLISECONDS);

                // build the ocsp cache
                ocspCache = Caffeine.newBuilder().expireAfterWrite(cacheDurationMillis, TimeUnit.MILLISECONDS).build(ocspRequest -> {
                    final String subjectDn = ocspRequest.getSubjectCertificate().getSubjectX500Principal().getName();

                    logger.info("Validating client certificate via OCSP: <{}>", subjectDn);
                    final OcspStatus ocspStatus = getOcspStatus(ocspRequest);
                    logger.info("Client certificate status for <{}>: {}", subjectDn, ocspStatus);

                    return ocspStatus;
                });
            } catch (final Exception e) {
                logger.error("Disabling OCSP certificate validation. Unable to load OCSP configuration", e);
                client = null;
            }
        }
    }

    /**
     * Loads the ocsp certificate if specified. Null otherwise.
     *
     * @param properties nifi properties
     * @return certificate
     */
    private X509Certificate getOcspCertificate(final NiFiProperties properties) {
        X509Certificate validationAuthorityCertificate = null;

        final String validationAuthorityCertificatePath = properties.getProperty(NiFiProperties.SECURITY_OCSP_RESPONDER_CERTIFICATE);
        if (StringUtils.isNotBlank(validationAuthorityCertificatePath)) {
            try (final FileInputStream fis = new FileInputStream(validationAuthorityCertificatePath)) {
                final CertificateFactory cf = CertificateFactory.getInstance("X.509");
                validationAuthorityCertificate = (X509Certificate) cf.generateCertificate(fis);
            } catch (final Exception e) {
                throw new IllegalStateException("Unable to load the validation authority certificate: " + e);
            }
        }

        return validationAuthorityCertificate;
    }

    /**
     * Loads the trusted certificate authorities according to the specified properties.
     *
     * @param properties properties
     * @return map of certificate authorities
     */
    private Map<String, X509Certificate> getTrustedCAs(final NiFiProperties properties) {
        final Map<String, X509Certificate> certificateAuthorities = new HashMap<>();

        // get the path to the truststore
        final String truststorePath = properties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE);
        if (truststorePath == null) {
            throw new IllegalArgumentException("The truststore path is required.");
        }

        // get the truststore password
        final char[] truststorePassword;
        final String rawTruststorePassword = properties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD);
        if (rawTruststorePassword == null) {
            truststorePassword = new char[0];
        } else {
            truststorePassword = rawTruststorePassword.toCharArray();
        }

        // load the configured truststore
        try (final FileInputStream fis = new FileInputStream(truststorePath)) {
            final KeyStore truststore = KeyStore.getInstance(KeyStore.getDefaultType());
            truststore.load(fis, truststorePassword);

            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(truststore);

            // consider any certificates in the truststore as a trusted ca
            for (TrustManager trustManager : trustManagerFactory.getTrustManagers()) {
                if (trustManager instanceof X509TrustManager) {
                    for (X509Certificate ca : ((X509TrustManager) trustManager).getAcceptedIssuers()) {
                        certificateAuthorities.put(ca.getSubjectX500Principal().getName(), ca);
                    }
                }
            }
        } catch (final Exception e) {
            throw new IllegalStateException("Unable to load the configured truststore: " + e);
        }

        return certificateAuthorities;
    }

    /**
     * Validates the specified certificate using OCSP if configured.
     *
     * @param certificates the client certificates
     * @throws CertificateStatusException ex
     */
    public void validate(final X509Certificate[] certificates) throws CertificateStatusException {
        // only validate if configured to do so
        if (client != null && certificates != null && certificates.length > 0) {
            final X509Certificate subjectCertificate = getSubjectCertificate(certificates);
            final X509Certificate issuerCertificate = getIssuerCertificate(certificates);
            if (issuerCertificate == null) {
                throw new IllegalArgumentException(String.format("Unable to obtain certificate of issuer <%s> for the specified subject certificate <%s>.",
                        subjectCertificate.getIssuerX500Principal().getName(), subjectCertificate.getSubjectX500Principal().getName()));
            }

            // create the ocsp status key
            final OcspRequest ocspRequest = new OcspRequest(subjectCertificate, issuerCertificate);

            // determine the status and ensure it isn't verified as revoked
            final OcspStatus ocspStatus = ocspCache.get(ocspRequest);

            // we only disallow when we have a verified response that states the certificate is revoked
            if (VerificationStatus.Verified.equals(ocspStatus.getVerificationStatus()) && ValidationStatus.Revoked.equals(ocspStatus.getValidationStatus())) {
                throw new CertificateStatusException(String.format("Client certificate for <%s> is revoked according to the certificate authority.",
                        subjectCertificate.getSubjectX500Principal().getName()));
            }
        }
    }

    /**
     * Gets the subject certificate.
     *
     * @param certificates certs
     * @return subject cert
     */
    private X509Certificate getSubjectCertificate(final X509Certificate[] certificates) {
        return certificates[0];
    }

    /**
     * Gets the issuer certificate.
     *
     * @param certificates certs
     * @return issuer cert
     */
    private X509Certificate getIssuerCertificate(final X509Certificate[] certificates) {
        if (certificates.length > 1) {
            return certificates[1];
        } else if (certificates.length == 1) {
            final X509Certificate subjectCertificate = getSubjectCertificate(certificates);
            final X500Principal issuerPrincipal = subjectCertificate.getIssuerX500Principal();
            return trustedCAs.get(issuerPrincipal.getName());
        } else {
            return null;
        }
    }

    /**
     * Gets the OCSP status for the specified subject and issuer certificates.
     *
     * @param ocspStatusKey status key
     * @return ocsp status
     */
    private OcspStatus getOcspStatus(final OcspRequest ocspStatusKey) {
        final X509Certificate subjectCertificate = ocspStatusKey.getSubjectCertificate();
        final X509Certificate issuerCertificate = ocspStatusKey.getIssuerCertificate();

        // initialize the default status
        final OcspStatus ocspStatus = new OcspStatus();
        ocspStatus.setVerificationStatus(VerificationStatus.Unknown);
        ocspStatus.setValidationStatus(ValidationStatus.Unknown);

        try {
            // prepare the request
            final BigInteger subjectSerialNumber = subjectCertificate.getSerialNumber();
            final DigestCalculatorProvider calculatorProviderBuilder = new JcaDigestCalculatorProviderBuilder().setProvider("BC").build();
            final CertificateID certificateId = new CertificateID(calculatorProviderBuilder.get(CertificateID.HASH_SHA1),
                    new X509CertificateHolder(issuerCertificate.getEncoded()),
                    subjectSerialNumber);

            // generate the request
            final OCSPReqBuilder requestGenerator = new OCSPReqBuilder();
            requestGenerator.addRequest(certificateId);

            // Create a nonce to avoid replay attack
            BigInteger nonce = BigInteger.valueOf(System.currentTimeMillis());
            Extension ext = new Extension(OCSPObjectIdentifiers.id_pkix_ocsp_nonce, true, new DEROctetString(nonce.toByteArray()));
            requestGenerator.setRequestExtensions(new Extensions(new Extension[]{ext}));

            final OCSPReq ocspRequest = requestGenerator.build();

            // perform the request
            final Response response = getClientResponse(ocspRequest);

            // ensure the request was completed successfully
            if (Response.Status.OK.getStatusCode() != response.getStatusInfo().getStatusCode()) {
                logger.warn("OCSP request was unsuccessful ({}).", response.getStatus());
                return ocspStatus;
            }

            // interpret the response
            OCSPResp ocspResponse = new OCSPResp(response.readEntity(InputStream.class));

            // verify the response status
            switch (ocspResponse.getStatus()) {
                case OCSPRespBuilder.SUCCESSFUL:
                    ocspStatus.setResponseStatus(OcspStatus.ResponseStatus.Successful);
                    break;
                case OCSPRespBuilder.INTERNAL_ERROR:
                    ocspStatus.setResponseStatus(OcspStatus.ResponseStatus.InternalError);
                    break;
                case OCSPRespBuilder.MALFORMED_REQUEST:
                    ocspStatus.setResponseStatus(OcspStatus.ResponseStatus.MalformedRequest);
                    break;
                case OCSPRespBuilder.SIG_REQUIRED:
                    ocspStatus.setResponseStatus(OcspStatus.ResponseStatus.SignatureRequired);
                    break;
                case OCSPRespBuilder.TRY_LATER:
                    ocspStatus.setResponseStatus(OcspStatus.ResponseStatus.TryLater);
                    break;
                case OCSPRespBuilder.UNAUTHORIZED:
                    ocspStatus.setResponseStatus(OcspStatus.ResponseStatus.Unauthorized);
                    break;
                default:
                    ocspStatus.setResponseStatus(OcspStatus.ResponseStatus.Unknown);
                    break;
            }

            // only proceed if the response was successful
            if (ocspResponse.getStatus() != OCSPRespBuilder.SUCCESSFUL) {
                logger.warn("OCSP request was unsuccessful ({}).", ocspStatus.getResponseStatus());
                return ocspStatus;
            }

            // ensure the appropriate response object
            final Object ocspResponseObject = ocspResponse.getResponseObject();
            if (!(ocspResponseObject instanceof BasicOCSPResp)) {
                logger.warn("Unexpected OCSP response object: {}", ocspResponseObject);
                return ocspStatus;
            }

            // get the response object
            final BasicOCSPResp basicOcspResponse = (BasicOCSPResp) ocspResponse.getResponseObject();

            // attempt to locate the responder certificate
            final X509CertificateHolder[] responderCertificates = basicOcspResponse.getCerts();
            if (responderCertificates.length != 1) {
                logger.warn("Unexpected number of OCSP responder certificates: {}", responderCertificates.length);
                return ocspStatus;
            }

            // get the responder certificate
            final X509Certificate trustedResponderCertificate = getTrustedResponderCertificate(responderCertificates[0], issuerCertificate);
            if (trustedResponderCertificate != null) {
                // verify the response
                if (basicOcspResponse.isSignatureValid(new JcaContentVerifierProviderBuilder().setProvider("BC").build(trustedResponderCertificate.getPublicKey()))) {
                    ocspStatus.setVerificationStatus(VerificationStatus.Verified);
                } else {
                    ocspStatus.setVerificationStatus(VerificationStatus.Unverified);
                }
            } else {
                ocspStatus.setVerificationStatus(VerificationStatus.Unverified);
            }

            // validate the response
            final SingleResp[] responses = basicOcspResponse.getResponses();
            for (SingleResp singleResponse : responses) {
                final CertificateID responseCertificateId = singleResponse.getCertID();
                final BigInteger responseSerialNumber = responseCertificateId.getSerialNumber();

                if (responseSerialNumber.equals(subjectSerialNumber)) {
                    Object certStatus = singleResponse.getCertStatus();

                    // interpret the certificate status
                    if (CertificateStatus.GOOD == certStatus) {
                        ocspStatus.setValidationStatus(ValidationStatus.Good);
                    } else if (certStatus instanceof RevokedStatus) {
                        ocspStatus.setValidationStatus(ValidationStatus.Revoked);
                    } else {
                        ocspStatus.setValidationStatus(ValidationStatus.Unknown);
                    }
                }
            }
        } catch (final OCSPException | IOException | ProcessingException | OperatorCreationException e) {
            logger.error(e.getMessage(), e);
        } catch (CertificateException e) {
            logger.error("Certificate processing failed", e);
        }

        return ocspStatus;
    }

    private Response getClientResponse(OCSPReq ocspRequest) throws IOException {
        final WebTarget webTarget = client.target(validationAuthorityURI);
        return webTarget.request().post(Entity.entity(ocspRequest.getEncoded(), OCSP_REQUEST_CONTENT_TYPE));
    }

    /**
     * Gets the trusted responder certificate. The response contains the responder certificate, however we cannot blindly trust it. Instead, we use a configured trusted CA. If the responder
     * certificate is a trusted CA, then we can use it. If the responder certificate is not directly trusted, we still may be able to trust it if it was issued by the same CA that issued the subject
     * certificate. Other various checks may be required (this portion is currently not implemented).
     *
     * @param responderCertificateHolder cert
     * @param issuerCertificate          cert
     * @return cert
     */
    private X509Certificate getTrustedResponderCertificate(final X509CertificateHolder responderCertificateHolder, final X509Certificate issuerCertificate) throws CertificateException {
        // look for the responder's certificate specifically
        final X509Certificate responderCertificate = new JcaX509CertificateConverter().setProvider("BC").getCertificate(responderCertificateHolder);
        final String trustedCAName = responderCertificate.getSubjectX500Principal().getName();
        if (trustedCAs.containsKey(trustedCAName)) {
            return trustedCAs.get(trustedCAName);
        }

        // if the responder certificate was issued by the same CA that issued the subject certificate we may be able to use that...
        final X500Principal issuerCA = issuerCertificate.getSubjectX500Principal();
        if (responderCertificate.getIssuerX500Principal().equals(issuerCA)) {
            return null;
        } else {
            return null;
        }
    }
}
