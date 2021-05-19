package raptor.spark.demo.hive;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveMain {

  private static final Logger logger = LoggerFactory.getLogger(HiveMain.class);


  public static void clean(SparkSession spark) {
    logger.info("cleaning ...");
    spark.sql("USE default").show();
    spark.sql("SHOW DATABASES").show();
    spark.sql("DROP TABLE IF EXISTS mydb.mytable ").show();
    spark.sql("DROP DATABASE IF EXISTS mydb").show();
    spark.sql("SHOW DATABASES").show();
  }

  public static void main(String[] args) {
    logger.info("Starting ");

    SparkSession spark = SparkSession
        .builder()
        .appName("Java Spark Hive Example")
        .enableHiveSupport()
        .getOrCreate();

    spark.sql("SHOW DATABASES").show();
    spark.sql("SHOW TABLES").show();

    clean(spark);

    spark.sql("CREATE DATABASE IF NOT EXISTS mydb ").show();
    spark.sql("SHOW DATABASES").show();

    spark.sql("CREATE TABLE IF NOT EXISTS mydb.mytable (name string, value double) STORED AS PARQUET ")
        .show();
    spark.sql("USE mydb");
    spark.sql("SHOW TABLES").show();

    spark.sql("SELECT * FROM mydb.mytable").show();

    spark.sql("INSERT INTO mydb.mytable (name , value) VALUES ('john', 99.9) ").show();
    spark.sql("SELECT * FROM mydb.mytable").show();

    spark.sql("INSERT INTO mydb.mytable (name , value) VALUES ('alex', 20.99) ").show();
    spark.sql("SELECT * FROM mydb.mytable").show();

    clean(spark);

    logger.info("Finished ");
    spark.stop();
  }
}
