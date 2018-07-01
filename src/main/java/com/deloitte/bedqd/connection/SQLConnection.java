package com.deloitte.bedqd.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SQLConnection {

	static final String JDBC_DRIVER = "org.h2.Driver";   
	static final String DB_URL = "jdbc:h2:~/test";
	static final String USER = "sa"; 
	static final String PASS = ""; 
	  	
	public static PreparedStatement preparedStatement = null ;
	public static ResultSet resultset = null ;
	
	public static  Connection connection() throws Exception {
		Connection connection = null;
		String connectionURL = null;
		try {
			Class.forName(JDBC_DRIVER);
			connection = DriverManager.getConnection(DB_URL,USER,PASS); 
		} catch (SQLException se) {
			System.out.println("Error " + se.getMessage());
		} catch (ClassNotFoundException e) {
			System.out.println("Error " + e.getMessage());
		} catch (Exception e) {
			System.out.println("Error " + e.getMessage());
		}
		return connection;
	}
	
	public static Connection open_Connection() throws Exception{
		Connection connection = null;
		if (connection == null) {
			connection = connection();
		}
		return connection;
	}
	
	public Connection begin_Transaction(Connection connection) throws Exception{
		connection.setAutoCommit(false);
		return connection;
	}
	
	public Connection commit_Transaction(Connection connection) throws Exception{
		connection.commit();
		return connection;
	}
	
	public Connection rollback_Transaction(Connection connection) throws Exception{
		connection.rollback();
		return connection;
	}
	
	@SuppressWarnings("null")
	public static Connection close_Connection(Connection connection) throws Exception{
		if (connection != null || !connection.isClosed()) {
			if(preparedStatement != null || !preparedStatement.isClosed()){
				preparedStatement.close();
			}
			connection.close();
		}
		return connection;
	}
	
	public static int data_Retrive(String sqlQuery , Connection connection) throws Exception {
		preparedStatement = null;
		resultset = null ;
		int flag ;
		try {
			System.out.println("SQL Query --->" +sqlQuery);
			preparedStatement = connection.prepareStatement(sqlQuery);
			resultset = preparedStatement.executeQuery();
			flag = 1 ;
		} catch (Exception e) {
			flag = 0 ;
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {		
		}
		return flag ;
	}
	
}
