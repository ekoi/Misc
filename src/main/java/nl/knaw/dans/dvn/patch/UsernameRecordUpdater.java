package nl.knaw.dans.dvn.patch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Hello world!
 *
 */
public class UsernameRecordUpdater {
	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "org.postgresql.Driver";
	static String DB_URL = "jdbc:postgresql://localhost:5432/";

	static String DB_NAME;
	// Database credentials
	static String USER;
	static String PASS;

	public static void main(String[] args) {
		System.out.println("Database name: ");
		Scanner in = new Scanner( System.in );
		DB_NAME = in.nextLine();
		DB_URL += DB_NAME;
		System.out.println("Database User: ");
		USER = in.nextLine();
		System.out.println("Database Password: ");
		in = new Scanner( System.in );
		PASS = in.nextLine();
		System.out.println("Database Name: " + DB_NAME + "\tUsername: " + USER + "\tPASSWORD: " + PASS);
		try {
			List<RecordHolder> records = getUpdatedRecordCandidates();
			System.out.println("Number of updated record candidates: "+ records.size());
			List<RecordHolder> newRecs = new ArrayList<RecordHolder>();
			for (RecordHolder rh : records) {

				
				
				if (rh.getEmail().equals(rh.getUsername())) {
					System.out.println("Update is not needed for ");
					System.out.print("ID: " + rh.getId());
					System.out.print(", Email: " + rh.getEmail());
					System.out.print(", username: " + rh.getUsername());
					System.out.println("\n");
				}else 
					newRecs.add(rh);
			}
			System.out.println("\n========= UPDATING RECORDS =======");
			System.out.println("Number of records: "+ newRecs.size());
			for (RecordHolder rh:newRecs) {
				// Display values
				System.out.print("ID: " + rh.getId());
				System.out.print(", Email: " + rh.getEmail());
				System.out.print(", username: " + rh.getUsername());
				System.out.println("\n");
			}
			
			updateUsername(newRecs);
			
		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
		} 
		System.out.println("Goodbye!");
	}// end main

	private static List<RecordHolder> getUpdatedRecordCandidates()
			throws ClassNotFoundException, SQLException {
		List<RecordHolder> list = new ArrayList<RecordHolder>();
		Connection conn;
		Statement stmt;
		Class.forName(JDBC_DRIVER);
		System.out.println("Connecting to database...");
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		System.out.println("Query statement...");
		stmt = conn.createStatement();
		String sql = "select * from vdcuser where active is true and encryptedpassword is null";
		ResultSet rs = stmt.executeQuery(sql);

		while (rs.next()) {
			int id = rs.getInt("id");
			String email = rs.getString("email");
			String username = rs.getString("username");
			list.add(new RecordHolder(id, email, username));
		}
		rs.close();
		stmt.close();
		conn.close();
		return list;
	}
	
	private static void updateUsername( List<RecordHolder> list)
			throws ClassNotFoundException, SQLException {
		Connection conn;
		Statement stmt;
		Class.forName(JDBC_DRIVER);
		System.out.println("Connecting to database...");
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		conn.setAutoCommit(true);
		System.out.println("Updating statement...");
		String sql = "UPDATE vdcuser SET username=? WHERE id=?";
		
		PreparedStatement ps =conn.prepareStatement(sql);
		for (RecordHolder rh : list) {
			ps.setString(1, rh.getEmail());
			ps.setInt(2, rh.getId());
			ps.addBatch();
		}
		int[] affectedRecords = ps.executeBatch();
		for (int i:affectedRecords)
		System.out.println("Updated records: " + i);
		ps.close();
		conn.close();
	}
	
	
}
class RecordHolder {
	int id;
	String email;
	String username;
	public RecordHolder(int id, String email, String username) {
		this.id=id;
		this.email = email;
		this.username = username;
	}
	public int getId() {
		return id;
	}
	public String getEmail() {
		return email;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	
}
