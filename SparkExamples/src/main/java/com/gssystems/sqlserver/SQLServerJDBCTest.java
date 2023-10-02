package com.gssystems.sqlserver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class SQLServerJDBCTest {
    public static void main(String[] args) throws Exception {
        String connectionURL = "jdbc:sqlserver://localhost:1433;DatabaseName=temperaturesdb;Encrypt=True;TrustServerCertificate=True";
        String user = "sa";
        String pw = "Ganesh20022002";
        System.out.println("Trying to connect to SQL Server");
        Connection conn = DriverManager.getConnection(connectionURL, user, pw);
        PreparedStatement st = conn.prepareStatement("select count(*) from temperatures");
        ResultSet rs = st.executeQuery();
        while( rs.next()) {
            System.out.println(" Total number of rows in temperatures table = " + rs.getLong(1));
        }
        conn.close();
    }
    
}
