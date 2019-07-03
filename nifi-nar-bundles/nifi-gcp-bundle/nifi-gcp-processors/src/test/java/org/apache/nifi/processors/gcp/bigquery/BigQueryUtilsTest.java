package org.apache.nifi.processors.gcp.bigquery;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Test;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Field.Mode;

public class BigQueryUtilsTest {

	@Test
	public void can_create_a_simple_boolean_schema() throws IOException {
        final String jsonRead = new String(
                Files.readAllBytes(Paths.get("src/test/resources/schemas/simple_boolean_schema.json"))
        );
        Field booleanField = Field.newBuilder("boolean", LegacySQLTypeName.BOOLEAN).setMode(Mode.NULLABLE).build();
		Schema expected = Schema.of(booleanField);
		Assert.assertEquals(BigQueryUtils.schemaFromString(jsonRead), expected);
	}

	@Test(expected = BadTypeNameException.class)
	public void can_create_a_simple_record_schema_with_bad_type_case() throws IOException {
        final String jsonRead = new String(
                Files.readAllBytes(Paths.get("src/test/resources/schemas/simple_record_schema_with_bad_type_case.json"))
        );
        BigQueryUtils.schemaFromString(jsonRead);
	}

	@Test
	public void can_create_a_simple_record_schema() throws IOException {
        final String jsonRead = new String(
                Files.readAllBytes(Paths.get("src/test/resources/schemas/simple_record_schema.json"))
        );
        Field booleanField = Field.newBuilder("boolean", LegacySQLTypeName.BOOLEAN).setMode(Mode.NULLABLE).build();
		Schema expected = Schema.of(Field.of("Consent", LegacySQLTypeName.RECORD,
        		booleanField
        		).toBuilder().setMode(Mode.NULLABLE).build());
		Assert.assertEquals(BigQueryUtils.schemaFromString(jsonRead), expected);
	}

}
