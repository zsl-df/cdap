/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.common;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.plugin.PluginConfigurer;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.plugin.PluginSelector;
import co.cask.cdap.etl.api.Engine;
import co.cask.cdap.etl.api.MultiInputPipelineConfigurer;
import co.cask.cdap.etl.api.MultiInputStageConfigurer;
import co.cask.cdap.etl.api.MultiOutputPipelineConfigurer;
import co.cask.cdap.etl.api.MultiOutputStageConfigurer;
import co.cask.cdap.etl.api.PipelineConfigurer;

/**
 * Configurer for a pipeline, that delegates all operations to a PluginConfigurer, except it prefixes plugin ids
 * to provide isolation for each etl stage. For example, a source can use a plugin with id 'jdbcdriver' and
 * a sink can also use a plugin with id 'jdbcdriver' without clobbering each other.
 *
 * @param <C> type of the platform configurer
 */
public class InteractivePipelineConfigurer<C extends PluginConfigurer>
  implements PipelineConfigurer, MultiInputPipelineConfigurer, MultiOutputPipelineConfigurer {
  private final Engine engine;
  private final C configurer;
  private final String stageName;
  private final DefaultStageConfigurer stageConfigurer;
  private final Map<String, String> properties;

  public InteractivePipelineConfigurer(C configurer, String stageName, Engine engine) {
    this.configurer = configurer;
    this.stageName = stageName;
    this.stageConfigurer = new DefaultStageConfigurer();
    this.engine = engine;
    this.properties = new HashMap<>();
  }


  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties) {
    return configurer.usePlugin(pluginType, pluginName, getPluginId(pluginId), properties);
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                         PluginSelector selector) {
    return configurer.usePlugin(pluginType, pluginName, pluginId, properties, selector);
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId,
                                     PluginProperties properties) {
    return configurer.usePluginClass(pluginType, pluginName, getPluginId(pluginId), properties);
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                                     PluginSelector selector) {
    return configurer.usePluginClass(pluginType, pluginName, pluginId, properties, selector);
  }
  
  private String getPluginId(String childPluginId) {
    return String.format("%s%s%s", stageName, Constants.ID_SEPARATOR, childPluginId);
  }

  @Override
  public DefaultStageConfigurer getStageConfigurer() {
    return stageConfigurer;
  }

  @Override
  public Engine getEngine() {
    return engine;
  }

  @Override
  public void setPipelineProperties(Map<String, String> properties) {
    this.properties.clear();
    this.properties.putAll(properties);
  }

  @Override
  public MultiInputStageConfigurer getMultiInputStageConfigurer() {
    return stageConfigurer;
  }

  @Override
  public MultiOutputStageConfigurer getMultiOutputStageConfigurer() {
    return stageConfigurer;
  }

  public Map<String, String> getPipelineProperties() {
    return properties;
  }


@Override
public void addStream(Stream stream) {
	// TODO Auto-generated method stub
	
}


@Override
public void addStream(String streamName) {
	// TODO Auto-generated method stub
	
}


@Override
public void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {
	// TODO Auto-generated method stub
	
}


@Override
public void addDatasetType(Class<? extends Dataset> datasetClass) {
	// TODO Auto-generated method stub
	
}


@Override
public void createDataset(String datasetName, String typeName, DatasetProperties properties) {
	// TODO Auto-generated method stub
	
}


@Override
public void createDataset(String datasetName, String typeName) {
	// TODO Auto-generated method stub
	
}


@Override
public void createDataset(String datasetName, Class<? extends Dataset> datasetClass, DatasetProperties props) {
	// TODO Auto-generated method stub
	
}


@Override
public void createDataset(String datasetName, Class<? extends Dataset> datasetClass) {
	// TODO Auto-generated method stub
	
}
}
