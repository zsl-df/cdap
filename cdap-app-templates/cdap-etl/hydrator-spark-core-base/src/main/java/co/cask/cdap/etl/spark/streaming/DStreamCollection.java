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

package co.cask.cdap.etl.spark.streaming;

import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.etl.api.Alert;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.api.dataframe.SparkDataframeCompute;
import co.cask.cdap.etl.api.dataframe.SparkDataframeSink;
import co.cask.cdap.etl.api.dataframe.SparkDataframeSource;
import co.cask.cdap.etl.api.streaming.Windower;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.NoopStageStatisticsCollector;
import co.cask.cdap.etl.common.PipelineRuntime;
import co.cask.cdap.etl.common.RecordInfo;
import co.cask.cdap.etl.common.StageStatisticsCollector;
import co.cask.cdap.etl.spark.Compat;
import co.cask.cdap.etl.spark.SparkCollection;
import co.cask.cdap.etl.spark.SparkPairCollection;
import co.cask.cdap.etl.spark.SparkPipelineRuntime;
import co.cask.cdap.etl.spark.batch.BasicSparkExecutionPluginContext;
import co.cask.cdap.etl.spark.streaming.function.*;
import co.cask.cdap.etl.spec.StageSpec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import javax.annotation.Nullable;

/**
 * JavaDStream backed {@link co.cask.cdap.etl.spark.SparkCollection}
 *
 * @param <T> type of objects in the collection
 */
public class DStreamCollection<T> implements SparkCollection<T> {

  private final JavaSparkExecutionContext sec;
  private final JavaDStream<T> stream;

  public DStreamCollection(JavaSparkExecutionContext sec, JavaDStream<T> stream) {
    this.sec = sec;
    this.stream = stream;
  }

  @SuppressWarnings("unchecked")
  @Override
  public JavaDStream<T> getUnderlying() {
    return stream;
  }

  @Override
  public SparkCollection<T> cache() {
    SparkConf sparkConf = stream.context().sparkContext().getConf();
    if (sparkConf.getBoolean(Constants.SPARK_PIPELINE_AUTOCACHE_ENABLE_FLAG, true)) {
      String cacheStorageLevelString = sparkConf.get(Constants.SPARK_PIPELINE_CACHING_STORAGE_LEVEL, 
                                                     Constants.DEFAULT_CACHING_STORAGE_LEVEL);
      StorageLevel cacheStorageLevel = StorageLevel.fromString(cacheStorageLevelString);
      return wrap(stream.persist(cacheStorageLevel));
    } else {
      return wrap(stream);
    }
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public SparkCollection<T> union(SparkCollection<T> other) {
    return wrap(stream.union((JavaDStream<T>) other.getUnderlying()));
  }

  @Override
  public SparkCollection<RecordInfo<Object>> transform(StageSpec stageSpec, StageStatisticsCollector collector) {
    return wrap(stream.transform(new DynamicTransform<T>(new DynamicDriverContext(stageSpec, sec, collector), false)));
  }

  @Override
  public SparkCollection<RecordInfo<Object>> multiOutputTransform(StageSpec stageSpec,
                                                                  StageStatisticsCollector collector) {
    return wrap(stream.transform(new DynamicTransform<T>(new DynamicDriverContext(stageSpec, sec, collector), true)));
  }

  @Override
  public <U> SparkCollection<U> flatMap(StageSpec stageSpec, FlatMapFunction<T, U> function) {
    return wrap(stream.flatMap(function));
  }

  @Override
  public <K, V> SparkPairCollection<K, V> flatMapToPair(PairFlatMapFunction<T, K, V> function) {
    return new PairDStreamCollection<>(sec, stream.flatMapToPair(function));
  }

  @Override
  public <U> SparkCollection<U> load(StageSpec stageSpec, SparkDataframeSource<U> compute) throws Exception {
    throw new UnsupportedOperationException("load not supported in DStreamCollection");
  }

  @Override
  public SparkCollection<RecordInfo<Object>> aggregate(StageSpec stageSpec, @Nullable Integer partitions,
                                                       StageStatisticsCollector collector) {
    DynamicDriverContext dynamicDriverContext = new DynamicDriverContext(stageSpec, sec, collector);
    JavaPairDStream<Object, T> keyedCollection =
      stream.transformToPair(new DynamicAggregatorGroupBy<Object, T>(dynamicDriverContext));

    JavaPairDStream<Object, Iterable<T>> groupedCollection = partitions == null ?
      keyedCollection.groupByKey() : keyedCollection.groupByKey(partitions);

    return wrap(groupedCollection.transform(new DynamicAggregatorAggregate<Object, T, Object>(dynamicDriverContext)));
  }

  @Override
  public <U> SparkCollection<U> compute(final StageSpec stageSpec, SparkCompute<T, U> compute) throws Exception {
    //TODO sb temp commented, to work on batch first
    final SparkCompute<T, U> wrappedCompute =
      new DynamicSparkCompute(new DynamicDriverContext(stageSpec, sec, new NoopStageStatisticsCollector()), compute);
    Transactionals.execute(sec, new TxRunnable() {
      @Override
      public void run(DatasetContext datasetContext) throws Exception {

        PipelineRuntime pipelineRuntime = new SparkPipelineRuntime(sec);
        SparkSession sparkSession = SparkSession.builder().config(stream.context().sparkContext().getConf()).getOrCreate();
        SparkExecutionPluginContext sparkPluginContext =
          new BasicSparkExecutionPluginContext(sec, sparkSession,
                                               datasetContext, pipelineRuntime, stageSpec);
        wrappedCompute.initialize(sparkPluginContext);
      }
    }, Exception.class);
    return wrap(stream.transform(new ComputeTransformFunction<>(sec, stageSpec, wrappedCompute)));
  }

  @Override
  public <U> SparkCollection<U> compute(StageSpec stageSpec, SparkDataframeCompute<T, U> compute) throws Exception {
    //TODO sb temp commented, to work on batch first
    final SparkDataframeCompute<T, U> wrappedSparkDataframeCompute =
            new DynamicSparkDataframeCompute(new DynamicDriverContext(stageSpec, sec, new NoopStageStatisticsCollector()), compute);
    Transactionals.execute(sec, new TxRunnable() {
      @Override
      public void run(DatasetContext datasetContext) throws Exception {

        PipelineRuntime pipelineRuntime = new SparkPipelineRuntime(sec);
        SparkSession sparkSession = SparkSession.builder().config(stream.context().sparkContext().getConf()).getOrCreate();
        SparkExecutionPluginContext sparkPluginContext =
                new BasicSparkExecutionPluginContext(sec, sparkSession,
                        datasetContext, pipelineRuntime, stageSpec);
        wrappedSparkDataframeCompute.initialize(sparkPluginContext);
      }
    }, Exception.class);
    return wrap(stream.transform(new ComputeDataframeTransformFunction(sec, stageSpec, wrappedSparkDataframeCompute)));
  }

  @Override
  public Runnable createStoreTask(final StageSpec stageSpec,
                                  final PairFlatMapFunction<T, Object, Object> sinkFunction) {
    return new Runnable() {
      @Override
      public void run() {
        // cache since the streaming sink function will check if the rdd is empty, which can cause recomputation
        // and confusing metrics if its not cached.
        Compat.foreachRDD(stream.cache(), new StreamingBatchSinkFunction<>(sinkFunction, sec, stageSpec));
      }
    };
  }

  @Override
  public Runnable createStoreTask(final StageSpec stageSpec, SparkSink<T> sink) throws Exception {
    return new Runnable() {
      @Override
      public void run() {
        Compat.foreachRDD(stream.cache(), new StreamingSparkSinkFunction<T>(sec, stageSpec));
      }
    };
  }

  @Override
  public Runnable createStoreTask(StageSpec stageSpec, SparkDataframeSink<T> sink) throws Exception {
    throw new UnsupportedOperationException("not implemented yet");
  }

  @Override
  public void publishAlerts(final StageSpec stageSpec, StageStatisticsCollector collector) throws Exception {
    Compat.foreachRDD((JavaDStream<Alert>) stream, new StreamingAlertPublishFunction(sec, stageSpec));
  }

  @Override
  public SparkCollection<T> window(StageSpec stageSpec, Windower windower) {
    String stageName = stageSpec.getName();
    return wrap(stream.transform(new CountingTransformFunction<T>(stageName, sec.getMetrics(), "records.in", null))
                  .window(Durations.seconds(windower.getWidth()), Durations.seconds(windower.getSlideInterval()))
                  .transform(new CountingTransformFunction<T>(stageName, sec.getMetrics(), "records.out",
                                                             sec.getDataTracer(stageName))));
  }

  private <U> SparkCollection<U> wrap(JavaDStream<U> stream) {
    return new DStreamCollection<>(sec, stream);
  }
}
