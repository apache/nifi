package org.apache.nifi.kafka.shared.login;

import org.apache.nifi.context.PropertyContext;

import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;

public class AADLoginConfigProvider implements LoginConfigProvider {

    private static final String MODULE_CLASS = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule";


    @Override
    public String getConfiguration(PropertyContext context) {

        final LoginConfigBuilder builder = new LoginConfigBuilder(MODULE_CLASS, REQUIRED);

        return builder.build();
    }

}
