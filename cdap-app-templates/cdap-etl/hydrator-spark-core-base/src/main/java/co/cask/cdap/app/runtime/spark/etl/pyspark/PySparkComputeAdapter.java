package co.cask.cdap.app.runtime.spark.etl.pyspark;

import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import org.apache.spark.api.java.JavaRDD;

/**
 * 
 * @author manjinder.aulakh
 *
 * @param <IN>
 * @param <OUT>
 */
public class PySparkComputeAdapter<IN, OUT> extends SparkCompute<IN, OUT> {

  private SparkCompute<IN, OUT> scompute;
  private PySparkComputeInterface pySparkCompute;

  public PySparkComputeAdapter(SparkCompute<IN, OUT> scompute, PySparkComputeInterface pySparkCompute) {
    this.scompute = scompute;
    this.pySparkCompute = pySparkCompute;
  }

  /**
   * Configure a pipeline.
   *
   * @param pipelineConfigurer the configurer used to add required datasets and streams
   * @throws IllegalArgumentException if the given config is invalid
   */
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    scompute.configurePipeline(pipelineConfigurer);
  }

  /**
   * Initialize the plugin. Will be called before any calls to {@link #transform(SparkExecutionPluginContext, JavaRDD)}
   * are made.
   *
   * @param context {@link SparkExecutionPluginContext} for this job
   * @throws Exception if there is an error initializing
   */
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    scompute.initialize(context);
  }

  /**
   * Transform the input and return the output to be sent to the next stage in the pipeline.
   *
   * @param context {@link SparkExecutionPluginContext} for this job
   * @param input input data to be transformed
   * @throws Exception if there is an error during this method invocation
   */
  @Override
  public JavaRDD<OUT> transform(SparkExecutionPluginContext context, JavaRDD<IN> input) throws Exception {
    return pySparkCompute.transform(context, input);
  }

}
