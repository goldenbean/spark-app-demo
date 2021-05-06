package raptor.spark.launch;

import java.io.IOException;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Launcher {

  private static final Logger logger = LoggerFactory.getLogger(Launcher.class);

  public static void main(String[] args) throws IOException {

    logger.info("Spark Home [{}]", args[0]);

    SparkAppHandle handler = new SparkLauncher()
        .setAppName("hello-world")
        .setSparkHome(args[0])
        .setMaster("yarn")
        .setConf("spark.driver.memory", "2g")
        .setConf("spark.executor.memory", "1g")
        .setConf("spark.executor.cores", "3")
        .setAppResource("target/spark-app-demo-1.0.0.jar")
        .setMainClass("raptor.spark.demo.hello.HelloWorld")
        .addAppArgs("I came from Launcher")
        .setDeployMode("cluster")
        .startApplication(new SparkAppHandle.Listener() {
          @Override
          public void stateChanged(SparkAppHandle handle) {
            System.out.println("**********  state  changed  **********");
          }

          @Override
          public void infoChanged(SparkAppHandle handle) {
            System.out.println("**********  info  changed  **********");
          }
        });

    while (!"FINISHED".equalsIgnoreCase(handler.getState().toString()) && !"FAILED"
        .equalsIgnoreCase(handler.getState().toString())) {
      System.out.println("id    " + handler.getAppId());
      System.out.println("state " + handler.getState());

      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

  }
}
