## Testing Synapse development via a remote IDE

* Synapse comes with its own full fledged IDE and we can develop the SQL scripts and notebooks directly inside the Synapse IDE that is web based. However there are scenarios where we want to do most of the development in a familiar IDE like VSCODE and test things locally before we can do our final testing inside Synapse. This has the advantage that we can save some money and not have to spawn a large spark pool inside Synapse to cater to the needs of a lot of developers in parallel. As advertised by MSFT, the Synapse Spark pool is not really conducive for usage by many developers in parallel unless we spawn a massive cluster. 

* To test things out locally, we need to first install SPARK from the website. Tutorials are present that walk us through the process of installing SPARK on both windows and Ubuntu and we can follow those to get the java, spark etc. installed. We can run the spark pi starter sampleto test and make sure the local install of spark is working fine. 

* Then we can install VSCODE and the python / Jyupiter plugins to test local development of spark notebooks, and then try to connect them to the remote spark cluster for final testing. We might have to build the notebooks in such a way that when we do local testing, we reference the input and output datasets as local folders and then replace them when we test against spark which reads and writes to ADLS.

* Please take a look at <a href="./VenkyRemoteNotebook1.ipynb">this</a> notebook to get started with local spark development. 