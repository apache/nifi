package org.apache.nifi.dbcp;

import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.kerberos.KerberosUserService;

public final class KerberosContext {

    private final KerberosCredentialsService kerberosCredentialsService;
    private final KerberosUserService kerberosUserService;
    private final String kerberosPrincipal;
    private final String kerberosPassword;

    public KerberosContext(KerberosCredentialsService kerberosCredentialsService, KerberosUserService kerberosUserService, String kerberosPrincipal, String kerberosPassword) {
        this.kerberosCredentialsService = kerberosCredentialsService;
        this.kerberosUserService = kerberosUserService;
        this.kerberosPrincipal = kerberosPrincipal;
        this.kerberosPassword = kerberosPassword;
    }

    public KerberosCredentialsService getKerberosCredentialsService() {
        return kerberosCredentialsService;
    }

    public KerberosUserService getKerberosUserService() {
        return kerberosUserService;
    }

    public String getKerberosPrincipal() {
        return kerberosPrincipal;
    }

    public String getKerberosPassword() {
        return kerberosPassword;
    }
}
