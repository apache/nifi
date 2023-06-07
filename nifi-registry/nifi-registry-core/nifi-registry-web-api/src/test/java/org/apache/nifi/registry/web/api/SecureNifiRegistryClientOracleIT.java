package org.apache.nifi.registry.web.api;

import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit.jupiter.EnabledIf;

@EnabledIf(value = "${current.database.is.oracle}", loadContext = true)
@Sql(executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD, scripts = {"classpath:db/clearDB.sql", "classpath:db/oracle/FlowsIT.sql"})
public class SecureNifiRegistryClientOracleIT extends SecureNiFiRegistryClientITBase {

}
