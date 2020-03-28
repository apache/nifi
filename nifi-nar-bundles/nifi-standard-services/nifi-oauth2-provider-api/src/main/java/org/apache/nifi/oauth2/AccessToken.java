package org.apache.nifi.oauth2;

public class AccessToken {
    private String accessToken;
    private String refreshToken;
    private String tokenType;
    private Integer expires;
    private String scope;

    private Long fetchTime;

    public AccessToken() {}

    public AccessToken(String accessToken,
                       String refreshToken,
                       String tokenType,
                       Integer expires,
                       String scope) {
        this.accessToken = accessToken;
        this.tokenType = tokenType;
        this.refreshToken = refreshToken;
        this.expires = expires;
        this.scope = scope;
        this.fetchTime = System.currentTimeMillis();
    }

    public String getAccessToken() {
        return accessToken;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public String getTokenType() {
        return tokenType;
    }

    public Integer getExpires() {
        return expires;
    }

    public String getScope() {
        return scope;
    }

    public Long getFetchTime() {
        return fetchTime;
    }

    public boolean isExpired() {
        return System.currentTimeMillis() >= ( fetchTime + (expires * 1000) );
    }
}
