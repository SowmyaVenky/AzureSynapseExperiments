package com.gssystems.spark;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

public class SynapseJDBCTesting {
    public static void main(String[] args ) throws Exception {
        if( args.length != 2 ) {
            System.out.println("Usage java SynapseJDBCTesting <<user>> <<pw>>");
            System.exit(-1);            
        }

        String sqlToExecute = "SELECT " + 
        " latitude " +
        ", longitude " +
        ", max(temperature_2m) as maxtemp " +
        ", min(temperature_2m) as mintemp " +
        " from temperatures_external " +
        " group by " +
        " latitude, " +
        " longitude";

        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setServerName("venkysynapse1001-ondemand.sql.azuresynapse.net"); // Replace with your server name
        ds.setDatabaseName("TEMPERATURES_LAKEHOUSE"); // Replace with your database
        ds.setUser(args[0]); // Replace with your user name
        ds.setPassword(args[1]); // Replace with your password
        ds.setAuthentication("ActiveDirectoryPassword");

        try (Connection connection = ds.getConnection();
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sqlToExecute)) {
            while (rs.next()) {
                System.out.println( "Latitude : " + rs.getString("latitude") 
                + ", Longitude : " + rs.getString("longitude")
                + ", Max Temp : " + rs.getString("maxtemp")
                + ", Min temp : " + rs.getString("mintemp")
                );
            }
        }
    }
}
