package co.cask.cdap.app.runtime.spark.etl.pyspark;

import co.cask.cdap.app.runtime.spark.DefaultJavaSparkExecutionContext;
import co.cask.cdap.app.runtime.spark.SparkClassLoader;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import org.apache.spark.api.java.JavaRDD;

/**
 * 
 * @author manjinder.aulakh
 *
 */
public interface PySparkComputeInterface {

//  DefaultJavaSparkExecutionContext SPARKCLASSLOADER = null;

  /**
   * 
   * @param context
   * @param input
   * @return
   * @throws Exception
   */
  JavaRDD transform(SparkExecutionPluginContext context, JavaRDD input) throws Exception;
  
  /**
   * 
   * @return
   */
  String toString();
}
