package raptor.spark.demo.hello;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloWorld {

  private static final Logger logger = LoggerFactory.getLogger(HelloWorld.class);

  public static void main(String[] args) throws InterruptedException {
    logger.info("Starting ");

    SparkSession spark = SparkSession
        .builder()
        //.master("yarn")
        //.appName("hello-world")
        //.config("spark.some.config.option", "some-value")
        .getOrCreate();

    List<Person> persons = new ArrayList<>();

    persons.add(new Person("abc", 22, "male"));
    persons.add(new Person("bcd", 25, "male"));
    persons.add(new Person("def", 23, "female"));

    spark.createDataFrame(persons, Person.class).show(false);

    spark.close();

  }
}
