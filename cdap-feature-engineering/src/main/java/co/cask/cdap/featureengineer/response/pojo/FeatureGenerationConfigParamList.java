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
package co.cask.cdap.featureengineer.response.pojo;

import java.util.LinkedList;
import java.util.List;

/**
 * @author bhupesh.goel
 *
 */
public class FeatureGenerationConfigParamList {
    
    private List<FeatureGenerationConfigParam> configParamList;
    
    public FeatureGenerationConfigParamList() {
        this.configParamList = new LinkedList<>();
    }
    
    public void addConfigParam(final FeatureGenerationConfigParam configParam) {
        if (this.configParamList == null) {
            this.configParamList = new LinkedList<>();
        }
        this.configParamList.add(configParam);
    }
    
    /**
     * @return the configParamList
     */
    public List<FeatureGenerationConfigParam> getConfigParamList() {
        return configParamList;
    }
    
    /**
     * @param configParamList
     *            the configParamList to set
     */
    public void setConfigParamList(List<FeatureGenerationConfigParam> configParamList) {
        this.configParamList = configParamList;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((configParamList == null) ? 0 : configParamList.hashCode());
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
        FeatureGenerationConfigParamList other = (FeatureGenerationConfigParamList) obj;
        if (configParamList == null) {
            if (other.configParamList != null) {
                return false;
            }
        } else if (!configParamList.equals(other.configParamList)) {
            return false;
        }
        return true;
    }
    
    @Override
    public String toString() {
        return "FeatureGenerationConfigParamList [configParamList=" + configParamList + "]";
    }
    
}
