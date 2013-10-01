package com.continuuity.internal.app.runtime.batch;

import com.continuuity.common.logging.LoggingContextAccessor;
import com.google.common.base.Throwables;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Wraps user-defined implementation of {@link Mapper} class which allows perform extra configuration.
 */
public class MapperWrapper extends Mapper {

  public static final String ATTR_MAPPER_CLASS = "c.mapper.class";

  private static final Logger LOG = LoggerFactory.getLogger(MapperWrapper.class);

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    MapReduceContextProvider mrContextProvider = new MapReduceContextProvider(context);
    final BasicMapReduceContext basicMapReduceContext = mrContextProvider.get();

    // now that the context is created, we need to make sure to properly close all datasets of the context
    try {
      String userMapper = context.getConfiguration().get(ATTR_MAPPER_CLASS);
      Mapper delegate = createMapperInstance(context.getConfiguration().getClassLoader(), userMapper);

      // injecting runtime components, like datasets, etc.
      basicMapReduceContext.injectFields(delegate);

      LoggingContextAccessor.setLoggingContext(basicMapReduceContext.getLoggingContext());

      // this is a hook for periodic flushing of changes buffered by datasets (to avoid OOME)
      WrappedMapper.Context flushingContext = createAutoFlushingContext(context, basicMapReduceContext);

      delegate.run(flushingContext);

      // transaction is not finished, but we want all operations to be dispatched (some could be buffered in
      // memory by tx agent
      try {
        basicMapReduceContext.flushOperations();
      } catch (Exception e) {
        LOG.error("Failed to flush operations at the end of mapper of " + basicMapReduceContext.toString());
        throw Throwables.propagate(e);
      }
    } finally {
      basicMapReduceContext.close();
    }
  }

  private WrappedMapper.Context createAutoFlushingContext(final Context context,
                                                          final BasicMapReduceContext basicMapReduceContext) {
    // NOTE: we will change auto-flush to take into account size of buffered data, so no need to do/test a lot with
    //       current approach
    final int flushFreq = context.getConfiguration().getInt("c.mapper.flush.freq", 10000);

    @SuppressWarnings("unchecked")
    WrappedMapper.Context flushingContext = new WrappedMapper().new Context(context) {
      private int processedRecords = 0;

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean result = super.nextKeyValue();
        if (++processedRecords > flushFreq) {
          try {
            LOG.info("Flushing dataset operations...");
            basicMapReduceContext.flushOperations();
          } catch (Exception e) {
            LOG.error("Failed to persist changes", e);
            throw Throwables.propagate(e);
          }
          processedRecords = 0;
        }
        return result;
      }
    };
    return flushingContext;
  }

  private Mapper createMapperInstance(ClassLoader classLoader, String userMapper) {
    try {
      return (Mapper) classLoader.loadClass(userMapper).newInstance();
    } catch (Exception e) {
      LOG.error("Failed to create instance of the user-defined Mapper class: " + userMapper);
      throw Throwables.propagate(e);
    }
  }
}
