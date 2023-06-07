package org.apache.nifi.registry.web.api;

import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit.jupiter.EnabledIf;

@EnabledIf(value = "${current.database.is.not.oracle}", loadContext = true)
@Sql(executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD, scripts = {"classpath:db/clearDB.sql", "classpath:db/FlowsIT.sql"})
public class SecureNiFiRegistryClientIT extends SecureNiFiRegistryClientITBase {

}
