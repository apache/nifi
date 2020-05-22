package org.apache.nifi.security.credstore;

import org.apache.nifi.processor.exception.ProcessException;

import javax.crypto.spec.SecretKeySpec;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyStore;
import java.util.Map;

public class HadoopCredentialStore {

    private static final String CRED_STORE_PASSWORD_ENVVAR = "HADOOP_CREDSTORE_PASSWORD";
    private static final String CRED_STORE_PASSWORD_DEFAULT = "none";

    private final String credStoreLocation;
    private final KeyStore credStore;

    public HadoopCredentialStore(String credStoreLocation) {
        this.credStoreLocation = credStoreLocation;

        try {
            credStore = KeyStore.getInstance("JCEKS");
            credStore.load(null, null);
        } catch (Exception e) {
            throw new ProcessException("Unable to create credential store", e);
        }
    }

    public HadoopCredentialStore addCredential(String alias, String password) {
        try {
            SecretKeySpec passwordAsSecretKey = new SecretKeySpec(password.getBytes("UTF-8"), "AES");

            credStore.setKeyEntry(alias, passwordAsSecretKey, getCredStorePassword(), null);

            return this;
        } catch (Exception e) {
            throw new ProcessException("Unable to add credential to the store", e);
        }
    }

    public void save() {
        try {
            credStore.store(getCredStoreOutputStream(), getCredStorePassword());
        } catch (Exception e) {
            throw new ProcessException("Unable to save credential store", e);
        }
    }

    private FileOutputStream getCredStoreOutputStream() throws FileNotFoundException {
        try {
            return new FileOutputStream(new URI(credStoreLocation).getPath());
        } catch (URISyntaxException e) {
            return new FileOutputStream(credStoreLocation);
        }
    }

    private char[] getCredStorePassword() {
        Map<String, String> env = getSystemEnv();
        String credStorePassword = env.getOrDefault(CRED_STORE_PASSWORD_ENVVAR, CRED_STORE_PASSWORD_DEFAULT);
        return credStorePassword.toCharArray();
    }

    Map<String, String> getSystemEnv() {
        return System.getenv();
    }
}
