/*
 * Copyright © 2018 Cask Data, Inc.
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
package co.cask.cdap.featureengineer.proto;

/**
 * @author bhupesh.goel
 *
 */
public class PersistWranglerRequest {

	private String schema;
	private String pluginConfig;
	
	public String getSchema() {
		return schema;
	}
	
	public void setSchema(String schema) {
		this.schema = schema;
	}
	
	public String getPluginConfig() {
		return pluginConfig;
	}
	
	public void setPluginConfig(String pluginConfig) {
		this.pluginConfig = pluginConfig;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((pluginConfig == null) ? 0 : pluginConfig.hashCode());
		result = prime * result + ((schema == null) ? 0 : schema.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PersistWranglerRequest other = (PersistWranglerRequest) obj;
		if (pluginConfig == null) {
			if (other.pluginConfig != null)
				return false;
		} else if (!pluginConfig.equals(other.pluginConfig))
			return false;
		if (schema == null) {
			if (other.schema != null)
				return false;
		} else if (!schema.equals(other.schema))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "PersistWranglerRequest [schema=" + schema + ", pluginConfig=" + pluginConfig + "]";
	}
	
}
