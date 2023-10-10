## Movie Lens dataset processing with Spark and Synapse

* In this experiment we are going to use the kaggle movie dataset and ETL it to fit into a more relational model. That data is then put into Azure Synapse and analyzed. 

<img src="./movies/movies_001.png" />

<pre>
set JAVA_HOME=c:\Venky\jdk-11.0.15.10-hotspot
set PATH=%PATH%;c:\Venky\spark\bin;c:\Venky\apache-maven-3.8.6\bin
set SPARK_HOME=c:\Venky\spark
SET HADOOP_HOME=C:\Venky\AzureSynapseExperiments\SparkExamples

cd C:\Venky\AzureSynapseExperiments\SparkExamples
mvn clean package

# Process the main movies and ratings files. 
spark-submit --master local[4] --class com.gssystems.movies.MovieDataProcessor2 target\SparkExamples-1.0-SNAPSHOT.jar file:///C:/Venky/AzureSynapseExperiments/datafiles/movielens/movies_metadata.csv.gz file:///C:/Venky/AzureSynapseExperiments/datafiles/movielens/ratings.csv.gz

# Process related files for movies.
spark-submit --master local[4] --class com.gssystems.movies.MovieDataProcessor target\SparkExamples-1.0-SNAPSHOT.jar file:///C:/Venky/AzureSynapseExperiments/datafiles/movielens/movies_metadata.csv.gz

# Process credits
spark-submit --master local[4] --class com.gssystems.movies.CreditsProcessor target\SparkExamples-1.0-SNAPSHOT.jar file:///C:/Venky/AzureSynapseExperiments/datafiles/movielens/credits.csv.gz

# Process keywords
spark-submit --master local[4] --class com.gssystems.movies.KeywordsProcessor target\SparkExamples-1.0-SNAPSHOT.jar file:///C:/Venky/AzureSynapseExperiments/datafiles/movielens/keywords.csv.gz

# generate fake users
spark-submit --master local[4] --packages com.github.javafaker:javafaker:1.0.2 --class com.gssystems.movies.FakeUsersGenerator target\SparkExamples-1.0-SNAPSHOT.jar file:///C:/Venky/AzureSynapseExperiments/datafiles/movielens/ratings.csv.gz

</pre>

* This will read the movie metadata file that is in JSON format (complicated nested arrays), and shreds the data to a more relational friendly format. There are various entities at play here and the processing will take care of the shredding process. As we can see there are parquet files created with specific schemas that will resemble a normalized data model in a relational database.

* We will also generate a lot of fake users with the IDs that gave the ratings from the de-duped ratings file so that it can be used to test various dynamic data masking inside Synapse. This process took a pretty long time to run on my computer. 

<img src="./movies/movies_002.png" />

* We can upload all the files to ADLS using Azure storage explorer. This allows us to just copy entire directories without having to do much. If we do not want to install this, we can do the azcopy command and copy all the folders to ADLS.

<img src="./movies/movies_003.png" />

* We can see the data from inside the linked ADLS account in Synapse. 

<img src="./movies/movies_004.png" />

* We can start creating tables inside the dedicated pool we have and bulk load the datasets to create a data warehouse kind of setup inside the dedicated pool. We can create a pipeline after defining the required stored procs.

<img src="./movies/movies_005.png" />

<img src="./movies/movies_006.png" />

<img src="./movies/movies_007.png" />

* We need to make sure that the managed identity being used for running the pipeline has proper priviledges to run stored procedures inside the synapse dedicated pool. Otherwise we are going to see errors saying that the stored procedure was not able be loaded. 

* I ran the stored procedures manually in SQL and counted the data in each of the tables. There were some tables that did not work when I tried to generate the SQL script because it was not able to detect the schema. I had to create them manually. 

<img src="./movies/movies_008.png" />






