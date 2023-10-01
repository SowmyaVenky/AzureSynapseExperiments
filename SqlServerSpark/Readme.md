## SQL Server Spark Testing

* This experiment will use Spark to push records to a SQL Server instance running in Docker.

<pre>
 docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=Ganesh20022002" -p 1433:1433 -d mcr.microsoft.com/mssql/server:2022-latest

# Run the program to read from the delta format and push it to sql server.
#
spark-submit --master local[4] --packages io.delta:delta-core_2.12:2.2.0,com.microsoft.sqlserver:mssql-jdbc:12.4.1.jre11 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --class com.gssystems.sqlserver.TemperaturesLoader target\SparkExamples-1.0-SNAPSHOT.jar file:///C:/Venky/spring_tx_temps_delta/

</pre>