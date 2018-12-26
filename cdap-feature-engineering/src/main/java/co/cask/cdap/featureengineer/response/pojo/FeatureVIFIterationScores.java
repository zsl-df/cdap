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
public class FeatureVIFIterationScores {
    List<FeatureVIFIterationRow> featureVIFIterationRow;
    
    public FeatureVIFIterationScores() {
        this.featureVIFIterationRow = new LinkedList<>();
    }
    
    /**
     * @return the featureVIFIterationRow
     */
    public List<FeatureVIFIterationRow> getFeatureVIFIterationRow() {
        return featureVIFIterationRow;
    }
    
    /**
     * @param featureVIFIterationRow
     *            the featureVIFIterationRow to set
     */
    public void setFeatureVIFIterationRow(List<FeatureVIFIterationRow> featureVIFIterationRow) {
        this.featureVIFIterationRow = featureVIFIterationRow;
    }
    
    public void addFeatureVIFIterationRow(FeatureVIFIterationRow row) {
        this.featureVIFIterationRow.add(row);
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((featureVIFIterationRow == null) ? 0 : featureVIFIterationRow.hashCode());
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
        FeatureVIFIterationScores other = (FeatureVIFIterationScores) obj;
        if (featureVIFIterationRow == null) {
            if (other.featureVIFIterationRow != null) {
                return false;
            }
        } else if (!featureVIFIterationRow.equals(other.featureVIFIterationRow)) {
            return false;
        }
        return true;
    }
    
    @Override
    public String toString() {
        return "FeatureVIFIterationScores [featureVIFIterationRow=" + featureVIFIterationRow + "]";
    }
    
}
