package com.gssystems.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * This example will abstract out the communication from local computer to Azure
 * Event Hub via a table created using the Table API pointing to the Event Hub.
 * It will make the event hub look like a database.
 */
public class AzureEventHubTableAPI {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool params = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(params);
		//env.setRuntimeMode(RuntimeExecutionMode.BATCH);

		// Note that you can use reserved names in the column names as long as you
		// enclose them with this ` character.

		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		System.out.println("Creating table using DDL on tableEnv pointing to the files in the local file system.");
		String ddl = "CREATE TABLE temperatures ( latitude double, longitude double, `time` string, temperature_2m double )"
				+ " with ( 'connector' =  'filesystem', 'format' = 'json', 'path' = '" + params.get("input") + "') ";
		System.out.println(ddl);
		TableResult result1 = tableEnv.executeSql(ddl);
		result1.print();

		TableResult result2 = tableEnv
				.executeSql("Select latitude, longitude, max(temperature_2m) as maxim, min(temperature_2m) as minim "
						+ " from temperatures "
						+ " where latitude = 13.0 and longitude = 77.600006 "
						+ " group by latitude, longitude");
		result2.print();

		// Now we will create another table to point directly to the kafka topic we
		// created and make it look like a table to flink!
		// NOTE THAT THE server is pointing to kafka:9093 not localhost:9092 since it is running inside the same docker container and network.
		//Changing to streaming since kafka is not supported in batch mode. - WONT WORK, first setting takes effect.
		//env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
		
		String ddl1 = "CREATE TABLE temperatures_kafka ( latitude double, longitude double, `time` string, temperature_2m double )"
				+ " with ( 'connector' =  'kafka', 'format' = 'json', 'topic' = 'temperatures', "
				+ " 'properties.bootstrap.servers' = 'kafka:9093', "
				+ " 'properties.group.id' = 'flink-consumer', 'scan.startup.mode' = 'earliest-offset' ) ";
		System.out.println(ddl1);
		TableResult result3 = tableEnv.executeSql(ddl1);
		result3.print();
		
		TableResult result4 = tableEnv
				.executeSql("Select latitude, longitude, max(temperature_2m) as maxim, min(temperature_2m) as minim "
						+ " from temperatures_kafka "
						+ " where latitude = 13.0 and longitude = 77.600006 "
						+ " group by latitude, longitude");
		result4.print();

	}
}
