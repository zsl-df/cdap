/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.spark.plugin;

import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchContext;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
//import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.dataframe.SparkDataframeSource;
import co.cask.cdap.etl.common.plugin.Caller;
import co.cask.cdap.etl.spark.dataframe.SparkDfSourceContext;
import org.apache.spark.sql.Dataset;

import java.util.concurrent.Callable;

/**
 * Wrapper around a {@link SparkCompute} that makes sure logging, classloading, and other pipeline capabilities
 * are setup correctly.
 *
 * @param <IN> Type of input object
 * @param <OUT> Type of output object
 */
public class WrappedSparkDataframeSource<OUT> extends SparkDataframeSource<OUT> {
  private final SparkDataframeSource<OUT> source;
  private final Caller caller;

  public WrappedSparkDataframeSource(SparkDataframeSource<OUT> source, Caller caller) {
    this.source = source;
    this.caller = caller;
  }

  @Override
  public void configurePipeline(final PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() {
        source.configurePipeline(pipelineConfigurer);
        return null;
      }
    });
  }

  @Override
  public void prepareRun(BatchContext context) throws Exception {
    caller.call(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        source.prepareRun(context);
        return null;
      }
    });
  }

//  @Override
//  public void prepareRun(SparkDfSourceContext context) throws Exception {
//    caller.call(new Callable<Void>() {
//      @Override
//      public Void call() throws Exception {
//        source.prepareRun(context);
//        return null;
//      }
//    });
//  }

  @Override
  public void initialize(final SparkExecutionPluginContext context) throws Exception {
    caller.call(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        source.initialize(context);
        return null;
      }
    });
  }

  @Override
  public Dataset load(final SparkExecutionPluginContext context) throws Exception {
    return caller.call(new Callable<Dataset>() {
      @Override
      public Dataset call() throws Exception {
        return source.load(context);
      }
    });
  }
}
