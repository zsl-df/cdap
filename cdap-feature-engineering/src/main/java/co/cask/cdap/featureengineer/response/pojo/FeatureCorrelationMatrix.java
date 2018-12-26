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
public class FeatureCorrelationMatrix {
    
    private List<FeatureCorrelationRow> featureCorrelationScores;
    
    public FeatureCorrelationMatrix() {
        this.featureCorrelationScores = new LinkedList<>();
    }
    
    /**
     * @return the featureCorrelationScores
     */
    public List<FeatureCorrelationRow> getFeatureCorrelationScores() {
        return featureCorrelationScores;
    }
    
    /**
     * @param featureCorrelationScores
     *            the featureCorrelationScores to set
     */
    public void setFeatureCorrelationScores(List<FeatureCorrelationRow> featureCorrelationScores) {
        this.featureCorrelationScores = featureCorrelationScores;
    }
    
    public void addFeatureCorrelationRow(final FeatureCorrelationRow featureCorrelationRow) {
        this.featureCorrelationScores.add(featureCorrelationRow);
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((featureCorrelationScores == null) ? 0 : featureCorrelationScores.hashCode());
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
        FeatureCorrelationMatrix other = (FeatureCorrelationMatrix) obj;
        if (featureCorrelationScores == null) {
            if (other.featureCorrelationScores != null) {
                return false;
            }
        } else if (!featureCorrelationScores.equals(other.featureCorrelationScores)) {
            return false;
        }
        return true;
    }
    
    @Override
    public String toString() {
        return "FeatureCorrelationMatrix [featureCorrelationScores=" + featureCorrelationScores + "]";
    }
    
}
