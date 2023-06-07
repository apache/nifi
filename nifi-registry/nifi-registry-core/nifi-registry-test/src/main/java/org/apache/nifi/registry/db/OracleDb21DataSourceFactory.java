package org.apache.nifi.registry.db;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.testcontainers.containers.OracleContainer;

@Configuration
@Profile("oracle-21")
public class OracleDb21DataSourceFactory extends OracleDbDataSourceFactory {

    private static final OracleContainer ORACLE_CONTAINER = new OracleContainer("gvenzl/oracle-xe:21-slim-faststart")
            .withDatabaseName("testDb")
            .withUsername("testUser")
            .withPassword("testPassword");

    static {
        ORACLE_CONTAINER.start();
    }

    @Override
    protected OracleContainer oracleContainer() {
        return ORACLE_CONTAINER;
    }
}
