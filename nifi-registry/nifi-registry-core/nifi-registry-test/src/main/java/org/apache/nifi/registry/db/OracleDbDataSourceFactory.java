package org.apache.nifi.registry.db;

import java.sql.SQLException;
import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import oracle.jdbc.datasource.OracleDataSource;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.delegate.DatabaseDelegate;
import org.testcontainers.jdbc.JdbcDatabaseDelegate;

public abstract class OracleDbDataSourceFactory extends TestDataSourceFactory {

    protected abstract OracleContainer oracleContainer();

    @Override
    protected DataSource createDataSource() {
        final OracleContainer oracleContainer = oracleContainer();
        try {
            final OracleDataSource dataSource = new oracle.jdbc.pool.OracleDataSource();
            dataSource.setURL(oracleContainer.getJdbcUrl());
            dataSource.setUser(oracleContainer.getUsername());
            dataSource.setPassword(oracleContainer.getPassword());
            dataSource.setDatabaseName(oracleContainer.getDatabaseName());
            return dataSource;
        } catch (SQLException e) {
            throw new RuntimeException("Unable to create Oracle data source", e);
        }
    }

    @PostConstruct
    public void initDatabase() {
        final DatabaseDelegate databaseDelegate = new JdbcDatabaseDelegate(oracleContainer(), "");
        databaseDelegate.execute("DROP DATABASE test; CREATE DATABASE test;", "", 0, false, true);
    }
}
