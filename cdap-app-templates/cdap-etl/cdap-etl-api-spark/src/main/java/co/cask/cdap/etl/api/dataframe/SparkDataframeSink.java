/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.etl.api.dataframe;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.etl.api.batch.BatchConfigurable;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import org.apache.spark.sql.Dataset;

import java.io.Serializable;


/**
 * SparkSink composes a final, optional stage of a Batch ETL Pipeline. In addition to configuring the Batch run, it
 * can also perform RDD operations on the key value pairs provided by the Batch run.
 *
 * {@link SparkDataframeSink#run} method is called inside the Batch Run while {@link SparkDataframeSink#prepareRun} and
 * {@link SparkDataframeSink#onRunFinish} methods are called on the client side, which launches the Batch run, before the
 * Batch run starts and after it finishes respectively.
 *
 * @param <IN> The type of input record to the SparkSink.
 */
@Beta
public abstract class SparkDataframeSink<IN> extends BatchConfigurable<SparkPluginContext> implements Serializable {

  //TODO change it to co.cask.cdap.etl.api.PipelineConfigurable

  public static final String PLUGIN_TYPE = "sparkdataframesink";
  private static final long serialVersionUID = -8846795220027037047L;


  /**
   * User Spark job which will be executed and is responsible for persisting any data.
   *
   * @param context {@link SparkExecutionPluginContext} for this job
   * @param input the input from previous stages of the Batch run.
   */
  public abstract void run(SparkExecutionPluginContext context, Dataset input) throws Exception;

}
