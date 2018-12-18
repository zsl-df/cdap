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
package co.cask.cdap.featureengineer.pipeline.pojo;

/**
 * @author bhupesh.goel
 *
 */
public class BasePipelineNode {
    public String name;
    public PluginNode plugin;
    public String errorDatasetName;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public PluginNode getPlugin() {
        return plugin;
    }

    public void setPlugin(PluginNode plugin) {
        this.plugin = plugin;
    }

    public String getErrorDatasetName() {
        return errorDatasetName;
    }

    public void setErrorDatasetName(String errorDatasetName) {
        this.errorDatasetName = errorDatasetName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((errorDatasetName == null) ? 0 : errorDatasetName.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((plugin == null) ? 0 : plugin.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        BasePipelineNode other = (BasePipelineNode) obj;
        if (errorDatasetName == null) {
            if (other.errorDatasetName != null) {
                return false;
            }
        } else if (!errorDatasetName.equals(other.errorDatasetName)) {
            return false;
        }
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        if (plugin == null) {
            if (other.plugin != null) {
                return false;
            }
        } else if (!plugin.equals(other.plugin)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "BasePipelineNode [name=" + name + ", plugin=" + plugin + ", errorDatasetName=" + errorDatasetName + "]";
    }

}
