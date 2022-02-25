# cycling-data-project
This project process cycling data from the Transport For London (TFL) website. 

## Data Warehouse
The Entity Relational Diagram (ERD) for the final Data Warehouse is represented in the following image:
![The ERD](/images/Cycling-ERD.png "ERD edited from dbdiagram.io")


## Apache Spark
In order to access S3 files locally with Spark, we added two `.jar` files to our pyspark jars folder: `hadoop-aws-*.jar` and `aws-java-sdk-*.jar`.


- After installing Pyspark `pip install pyspark`, we checked its version by running `pyspark.__version__`. 

- Then we also checked the Hadoop version that was installed alongside Spark by running:
```python
    sc._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
``` 

- We downloaded the `hadoop-aws-*.jar` with the same version as Hadoop from the [Apache Hadoop AWS package directory](https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/). In our case we downloaded [hadoop-aws-3.1.1.jar](https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.1/hadoop-aws-3.1.1.jar).

- Additionaly, we downloaded from [MVN Repository](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws), the [aws-java-sdk-1.12.165.jar](https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.165/aws-java-sdk-bundle-1.12.165.jar) which is compatible with the Hadoop version we have.



