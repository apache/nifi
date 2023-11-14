package org.apache.nifi.kafka.shared.aad;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;

import java.util.Date;
import java.util.Set;


public class OAuthBearerTokenImp implements OAuthBearerToken
{
    String token;
    long lifetimeMs;

    public OAuthBearerTokenImp(final String token, Date expiresOn) {
        this.token = token;
        this.lifetimeMs = expiresOn.getTime();
    }

    @Override
    public String value() {
        return this.token;
    }

    @Override
    public Set<String> scope() {
        return null;
    }

    @Override
    public long lifetimeMs() {
        return this.lifetimeMs;
    }

    @Override
    public String principalName() {
        return null;
    }

    @Override
    public Long startTimeMs() {
        return null;
    }
}
