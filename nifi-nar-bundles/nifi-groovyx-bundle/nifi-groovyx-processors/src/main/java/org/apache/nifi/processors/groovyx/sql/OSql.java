package org.apache.nifi.processors.groovyx.sql;

import groovy.sql.Sql;
import groovy.sql.InParameter;
import groovy.lang.GString;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.sql.SQLException;
import java.io.InputStream;
import java.io.Reader;

/***
 * class to simplify work with CLOB, BLOB, DATE, and TIMESTAMP types.
 * Allows following parameters set correctly Sql.BLOB(InputStream), Sql.CLOB(Reader), DATE(java.util.Date), TIMESTAMP(java.util.Date)
 */

//CHECKSTYLE:OFF
@SuppressWarnings("PMD")
public class OSql extends Sql {
	public OSql(Connection connection) {
		super(connection);
	}

	protected void setObject(PreparedStatement statement, int i, Object value) throws SQLException {
		try{
			if(value instanceof InParameter){
				InParameter p = (InParameter)value;
				if( p.getType()==Types.BLOB && p.getValue() instanceof InputStream ){
					statement.setBlob(i, (InputStream)p.getValue());
					return;
				}
				if( p.getType()==Types.CLOB && p.getValue() instanceof Reader ){
					statement.setClob(i, (Reader)p.getValue());
					return;
				}
				if( p.getType()==Types.DATE && p.getValue() instanceof java.util.Date && !(p.getValue() instanceof java.sql.Date) ){
					statement.setDate( i, new java.sql.Date( ((java.util.Date)p.getValue()).getTime() ) );
					return;
				}
				if( p.getType()==Types.TIMESTAMP && p.getValue() instanceof java.util.Date && !(p.getValue() instanceof java.sql.Timestamp) ){
					statement.setTimestamp( i, new java.sql.Timestamp( ((java.util.Date)p.getValue()).getTime() ) );
					return;
				}
			}
			if(value instanceof GString){
				value = value.toString();
			}
			super.setObject(statement, i, value);
		}catch(Exception e){
			throw new SQLException("Can't set a parameter #"+i+" to value type "+(value==null?"null":value.getClass().getName())+": "+e.getMessage(),e);
		}
	}
}
