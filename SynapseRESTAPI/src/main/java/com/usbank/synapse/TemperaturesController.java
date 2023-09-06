package com.usbank.synapse;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import java.util.List;
import java.util.ArrayList;
import com.usbank.synapse.beans.Temperature;

@RestController
public class TemperaturesController {

	/*
	 * Acloudguru keeps changing credentials for every session, taking it as
	 * parameters in the most unsecure way :-)
	 */
	@GetMapping("/minmax")
	public ResponseEntity<List<Temperature>> index(@RequestParam String id,
			@RequestParam String pw) {
		try {
			return new ResponseEntity<List<Temperature>>(getMinMaxTemperatures(id, pw), HttpStatus.OK);
		}catch(Exception ex) {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}
	}

	private List<Temperature> getMinMaxTemperatures(String id, String pw ) throws Exception {
        List<Temperature> retList = new ArrayList<Temperature>();
		
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
        ds.setUser(id); // Replace with your user name
        ds.setPassword(pw); // Replace with your password
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

				Temperature aTemperature = new Temperature();
				aTemperature.setLatitude(rs.getDouble("latitude"));
				aTemperature.setLongitude(rs.getDouble("longitude"));
				aTemperature.setMaxtemp(rs.getDouble("maxtemp"));
				aTemperature.setMintemp(rs.getDouble("mintemp"));
				retList.add(aTemperature);
            }
        }

		return retList;
    }
}
