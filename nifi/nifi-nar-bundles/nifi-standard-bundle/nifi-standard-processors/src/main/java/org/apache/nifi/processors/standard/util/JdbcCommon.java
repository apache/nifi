/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;


/**
 *  JDBC / SQL common functions.
 *
 */
public class JdbcCommon {

	public static long convertToAvroStream(ResultSet rs, OutputStream outStream) throws SQLException, IOException {
		
		Schema schema = createSchema(rs);
		GenericRecord rec = new GenericData.Record(schema);
		
//		ByteArrayOutputStream out = new ByteArrayOutputStream();
//		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		
		DatumWriter<GenericRecord> datumWriter  	= new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter= new DataFileWriter<GenericRecord>(datumWriter); 
		dataFileWriter.create(schema, outStream);
		
		ResultSetMetaData meta = rs.getMetaData();
		int nrOfColumns = meta.getColumnCount();
		long nrOfRows = 0;
		while (rs.next()) {
			for (int i = 1; i <= nrOfColumns; i++) {
				Object value = rs.getObject(i);
				rec.put(i-1, value);
			}
			dataFileWriter.append(rec);
			nrOfRows += 1;
		}

		dataFileWriter.close();
		return nrOfRows;
//		encoder.flush();
//		out.close();
//		byte[] serializedBytes = out.toByteArray();
//		return serializedBytes;
	}
	
	public static Schema createSchema(ResultSet rs) throws SQLException {
		
		ResultSetMetaData meta = rs.getMetaData();
		int nrOfColumns = meta.getColumnCount();
		String tableName = meta.getTableName(1);
		
		FieldAssembler<Schema> builder = SchemaBuilder.record(tableName).namespace("any.data").fields();
		
		/**
		 * 	Type conversion is not precise and is incomplete, needs to be fixed!!!!!!
		 */
		for (int i = 1; i <= nrOfColumns; i++)
			switch (meta.getColumnType(i)) {
			
			case java.sql.Types.CHAR:
			case java.sql.Types.LONGNVARCHAR:
			case java.sql.Types.LONGVARCHAR:
			case java.sql.Types.NCHAR:
			case java.sql.Types.NVARCHAR:
			case java.sql.Types.VARCHAR:
				builder.name(meta.getColumnName(i)).type().stringType().noDefault();			
				break;

			case java.sql.Types.BOOLEAN:
				builder.name(meta.getColumnName(i)).type().booleanType().noDefault();			
				break;

			case java.sql.Types.INTEGER:
			case java.sql.Types.SMALLINT:
			case java.sql.Types.TINYINT:
				builder.name(meta.getColumnName(i)).type().intType().noDefault();			
				break;

			case java.sql.Types.BIGINT:
				builder.name(meta.getColumnName(i)).type().longType().noDefault();			
				break;

			// java.sql.RowId is interface, is seems to be database implementation specific, let's convert to String
			case java.sql.Types.ROWID:
				builder.name(meta.getColumnName(i)).type().stringType().noDefault();			
				break;

			case java.sql.Types.FLOAT:
			case java.sql.Types.REAL:
				builder.name(meta.getColumnName(i)).type().floatType().noDefault();			
				break;

			case java.sql.Types.DOUBLE:
				builder.name(meta.getColumnName(i)).type().doubleType().noDefault();			
				break;

			// TODO Did not find direct suitable type, need to be clarified!!!!
			case java.sql.Types.DECIMAL:
			case java.sql.Types.NUMERIC:
				builder.name(meta.getColumnName(i)).type().stringType().noDefault();			
				break;

			// TODO Did not find direct suitable type, need to be clarified!!!!
			case java.sql.Types.DATE:
			case java.sql.Types.TIME:
			case java.sql.Types.TIMESTAMP:
				builder.name(meta.getColumnName(i)).type().stringType().noDefault();			
				break;

			default:
				break;
			}
		return builder.endRecord();
	}

}
