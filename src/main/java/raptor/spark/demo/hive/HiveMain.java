package raptor.spark.demo.hive;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveMain {

  private static final Logger logger = LoggerFactory.getLogger(HiveMain.class);

  public static void main(String[] args) {
    logger.info("Starting ");

    SparkSession spark = SparkSession
        .builder()
        .appName("Java Spark Hive Example")
        .enableHiveSupport()
        .getOrCreate();

    spark.sql("SHOW DATABASES").show();
    spark.sql("SHOW TABLES").show();

    logger.info("Finished ");
  }
}
