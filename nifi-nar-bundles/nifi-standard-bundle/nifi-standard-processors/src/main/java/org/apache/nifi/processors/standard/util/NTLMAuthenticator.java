package org.apache.nifi.processors.standard.util;

/*
 * Based on https://stackoverflow.com/questions/35620415/how-create-ntlm-authentification-with-retrofit
 *      and http://svn.apache.org/repos/asf/httpcomponents/httpclient/tags/4.5.2/httpclient/src/main/java/org/apache/http/impl/auth/NTLMEngineImpl.java
 */
import okhttp3.Authenticator;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

import java.io.IOException;
import java.util.List;

public class NTLMAuthenticator implements Authenticator {
    final PublicNTLMEngineImpl engine = new PublicNTLMEngineImpl();
    private final String domain;
    private final String username;
    private final String password;
    private final String ntlmMsg1;

    public NTLMAuthenticator(String username, String password, String domain) {
        this.domain = domain;
        this.username = username;
        this.password = password;
        String localNtlmMsg1 = null;
        try {
            localNtlmMsg1 = engine.generateType1Msg(null, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        ntlmMsg1 = localNtlmMsg1;
    }

    @Override
    public Request authenticate(Route route, Response response) throws IOException {
        final List<String> WWWAuthenticate = response.headers().values("WWW-Authenticate");
        if (WWWAuthenticate.contains("NTLM")) {
            return response.request().newBuilder().header("Authorization", "NTLM " + ntlmMsg1).build();
        }
        String ntlmMsg3 = null;
        try {
            ntlmMsg3 = engine.generateType3Msg(username, password, domain, "android-device", WWWAuthenticate.get(0).substring(5));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return response.request().newBuilder().header("Authorization", "NTLM " + ntlmMsg3).build();
    }
}
