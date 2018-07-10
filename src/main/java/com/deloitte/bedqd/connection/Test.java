package com.deloitte.bedqd.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Test {
	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "org.h2.Driver";
	static final String DB_URL = "jdbc:h2:tcp://10.118.44.228:8080/~/BANKDQ;MVCC=TRUE;AUTO_SERVER=TRUE;DB_CLOSE_ON_EXIT=FALSE";
	//static final String DB_URL = "jdbc:h2:~/BANKDQ;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false;";
	
	// Database credentials
	static final String USER = "BEDQD";
	static final String PASS = "bedqd";

	public static void main(String[] args) throws Exception {
		Connection conn = null;
		Statement stmt = null;
		try {
			Class.forName(JDBC_DRIVER);

			System.out.println("Connecting to a selected database...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			System.out.println("Connected database successfully...");

			PreparedStatement ps = conn
					.prepareStatement("select * from DQ_DATA_LAKE_MD");
			ResultSet r = ps.executeQuery();
			if (r.next()) {
				System.out.println("data?");
			}

			stmt = conn.createStatement();

			String sql = "SELECT * FROM BANKDQ.DQ_DATA_LAKE_MD";

			stmt.executeUpdate(sql);

			stmt.close();
			conn.close();
		} catch (SQLException se) {
			se.printStackTrace();
		}
	}
}
