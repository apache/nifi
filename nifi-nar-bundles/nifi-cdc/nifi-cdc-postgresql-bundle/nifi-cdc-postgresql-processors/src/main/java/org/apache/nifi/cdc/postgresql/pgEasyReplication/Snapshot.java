package org.apache.nifi.cdc.postgresql.pgEasyReplication;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

public class Snapshot {

	private String publication;
	
	public Snapshot(String pub) {
		this.publication = pub;
	}


	public ArrayList<String> getPublicationTables() throws SQLException {		
    	PreparedStatement stmt = ConnectionManager.getSQLConnection()
    			.prepareStatement("SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = ?");
    	
    	stmt.setString(1, this.publication);
    	ResultSet rs = stmt.executeQuery();
    	
    	ArrayList<String> pubTables = new ArrayList<String>();
    	
    	while(rs.next()) {
    		pubTables.add(rs.getString(1) + "." + rs.getString(2));
    	}
    	
    	rs.close();
    	stmt.close();
    	
    	return pubTables;
	}
	
	public ArrayList<String> getInitialSnapshotTable(String tableName) throws SQLException, IOException {	
		PGConnection pgcon = ConnectionManager.getSQLConnection().unwrap(PGConnection.class);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		
		CopyManager manager = pgcon.getCopyAPI();
		manager.copyOut("COPY (SELECT REGEXP_REPLACE(ROW_TO_JSON(t)::TEXT, '\\\\\\\\', '\\\\', 'g') FROM (SELECT * FROM " + tableName + ") t) TO STDOUT ", out);
		
		return new ArrayList<String>(Arrays.asList(out.toString("UTF-8").split("\n")));
	}
	
	public Event getInitialSnapshot() throws SQLException, IOException {	
		LinkedList<String> snapshot = new LinkedList<String>();
		
		ArrayList<String> pubTables = this.getPublicationTables();		
				
		for (String table : pubTables) {			
			ArrayList<String> lines = this.getInitialSnapshotTable(table);
			
			snapshot.add("{\"snaphost\":{\"" + table + "\":" + lines.toString().replace("\\\\\"", "\\\"") + "}}");
		}

		return new Event(snapshot, null, true, false, false);
	}
}
