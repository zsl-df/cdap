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

package co.cask.cdap.api.plugin;

import co.cask.cdap.api.annotation.Beta;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Contains information about a plugin class.
 */
@Beta
public class PluginClass {

  private final String type;
  private final String name;
  private final String description;
  private final String[] pluginInput;
  private final String[] pluginOutput;
  private final String[] pluginFunction;
  private final String className;
  private final String configFieldName;
  private final Map<String, PluginPropertyField> properties;
  private final Set<String> endpoints;

  public PluginClass(String type, String name, String description, String[] pluginInput, 
		  			 String[] pluginOutput, String[] pluginFunction,String className,
                     @Nullable String configfieldName, Map<String, PluginPropertyField> properties,
                     Set<String> endpoints) {
    if (type == null) {
      throw new IllegalArgumentException("Plugin class type cannot be null");
    }
    if (name == null) {
      throw new IllegalArgumentException("Plugin class name cannot be null");
    }
    if (description == null) {
      throw new IllegalArgumentException("Plugin class description cannot be null");
    }
    if (className == null) {
      throw new IllegalArgumentException("Plugin class className cannot be null");
    }
    if (properties == null) {
      throw new IllegalArgumentException("Plugin class properties cannot be null");
    }

    this.type = type;
    this.name = name;
    this.description = description;
    this.className = className;
    this.configFieldName = configfieldName;
    this.properties = properties;
    this.endpoints = endpoints;
    this.pluginInput = pluginInput;
    this.pluginOutput = pluginOutput;
    this.pluginFunction = pluginFunction;
  }

  public PluginClass(String type, String name, String description, String[] pluginInput, 
		  			 String[] pluginOutput,String[] pluginFunction, String className,
                     @Nullable String configfieldName, Map<String, PluginPropertyField> properties) {
    this(type, name, description, pluginInput, pluginOutput, pluginFunction, className, configfieldName, properties, new HashSet<>());
  }

  public PluginClass(String type, String name, String description, String className,
          @Nullable String configfieldName, Map<String, PluginPropertyField> properties) {
   this(type, name, description, null, null, null, className, configfieldName, properties, new HashSet<>());
  }
  
  /**
   * Returns the type name of the plugin.
   */
  public String getType() {
    return type;
  }

  /**
   * Returns name of the plugin.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns description of the plugin.
   */
  public String getDescription() {
    return description;
  }

  public String[] getPluginInput() {
	return pluginInput;
  }
	           
  public String[] getPluginOutput() {
	return pluginOutput;
  }
	    
  public String[] getPluginFunction() {
	return pluginFunction;
  }

  public String getPluginInputToString() {
	if(pluginInput == null) return "";
    StringBuilder sb = new StringBuilder();
    for(String info : pluginInput) {
      sb.append(info);
      sb.append(",");
    }
    return sb.toString();
  }
        
  public String getPluginOutputToString() {
	if(pluginOutput == null) return "";
    StringBuilder sb = new StringBuilder();
    for(String info : pluginOutput) {
      sb.append(info);
      sb.append(",");
    }
    return sb.toString();
  }
         
  public String getPluginFunctionToString() {
	if(pluginFunction == null) return "";
	StringBuilder sb = new StringBuilder();
    for(String info : pluginFunction) {
      sb.append(info);
      sb.append(",");
    }
    return sb.toString();
  }
  
  /**
   * Returns the fully qualified class name of the plugin.
   */
  public String getClassName() {
    return className;
  }

  /**
   * Returns the name of the field that extends from {@link PluginConfig} in the plugin class.
   * If no such field, {@code null} will be returned.
   */
  @Nullable
  public String getConfigFieldName() {
    return configFieldName;
  }

  /**
   * Returns the set of plugin endpoints available in the plugin.
   * If no such field will return empty set.
   */
  public Set<String> getEndpoints() {
    return endpoints;
  }

  /**
   * Returns a map from config property name to {@link PluginPropertyField} that are supported by the plugin class.
   */
  public Map<String, PluginPropertyField> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PluginClass that = (PluginClass) o;

    return Objects.equals(type, that.type)
      && Objects.equals(name, that.name)
      && Objects.equals(description, that.description)
      && Objects.equals(pluginInput == null ? "" : pluginInput, that.pluginInput == null ? "" : that.pluginInput) 
      && Objects.equals(pluginOutput == null ? "" : pluginOutput, that.pluginOutput == null ? "" : that.pluginOutput)
      && Objects.equals(pluginFunction == null ? "" : pluginFunction, that.pluginFunction == null ? "" : that.pluginFunction)
      && Objects.equals(className, that.className)
      && Objects.equals(configFieldName, that.configFieldName)
      && Objects.equals(properties, that.properties)
      && Objects.equals(endpoints, that.endpoints);
  }

  @Override
  public int hashCode() {
    int result = type.hashCode();
    result = 31 * result + name.hashCode();
    result = 31 * result + description.hashCode();
    result = 31 * result + ((pluginInput == null) ? 1 : pluginInput.hashCode());
    result = 31 * result + ((pluginOutput == null) ? 1 : pluginOutput.hashCode());
    result = 31 * result + ((pluginFunction == null) ? 1 : pluginFunction.hashCode());
    result = 31 * result + className.hashCode();
    result = 31 * result + (configFieldName != null ? configFieldName.hashCode() : 0);
    result = 31 * result + properties.hashCode();
    result = 31 * result + (endpoints != null ? endpoints.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "PluginClass{" +
      "className='" + className + '\'' +
      ", type='" + type + '\'' +
      ", name='" + name + '\'' +
      ", description='" + description + '\'' +
      ", pluginFunction='" + getPluginFunctionToString() + '\'' +
      ", pluginInput='" + getPluginInputToString() + '\'' +
      ", pluginOutput='" + getPluginOutputToString() + '\'' +
      ", configFieldName='" + configFieldName + '\'' +
      ", properties=" + properties + '\'' +
      ", endpoints='" + endpoints +
      '}';
  }
}
