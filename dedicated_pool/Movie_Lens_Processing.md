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

* We will also generate a lot of fake users with the IDs that gave the ratings from the de-duped ratings file so that it can be used to test various dynamic data masking inside Synapse.