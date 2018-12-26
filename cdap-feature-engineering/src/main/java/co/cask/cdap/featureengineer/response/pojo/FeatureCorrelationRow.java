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

import java.util.HashMap;
import java.util.Map;

/**
 * @author bhupesh.goel
 *
 */
public class FeatureCorrelationRow {
    private String featureName;
    private String coefficientType;
    private Map<String, Double> featureCorrelationScores;
    
    public FeatureCorrelationRow() {
        this.featureCorrelationScores = new HashMap<>();
    }
    
    /**
     * @return the featureName
     */
    public String getFeatureName() {
        return featureName;
    }
    
    /**
     * @param featureName
     *            the featureName to set
     */
    public void setFeatureName(String featureName) {
        this.featureName = featureName;
    }
    
    /**
     * @return the coefficientType
     */
    public String getCoefficientType() {
        return coefficientType;
    }
    
    /**
     * @param coefficientType
     *            the coefficientType to set
     */
    public void setCoefficientType(String coefficientType) {
        this.coefficientType = coefficientType;
    }
    
    /**
     * @return the featureCorrelationScores
     */
    public Map<String, Double> getFeatureCorrelationScores() {
        return featureCorrelationScores;
    }
    
    /**
     * @param featureCorrelationScores
     *            the featureCorrelationScores to set
     */
    public void setFeatureCorrelationScores(Map<String, Double> featureCorrelationScores) {
        this.featureCorrelationScores = featureCorrelationScores;
    }
    
    public void addFeatureCorrelationScore(final String featureName, final Double score) {
        this.featureCorrelationScores.put(featureName, score);
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((coefficientType == null) ? 0 : coefficientType.hashCode());
        result = prime * result + ((featureCorrelationScores == null) ? 0 : featureCorrelationScores.hashCode());
        result = prime * result + ((featureName == null) ? 0 : featureName.hashCode());
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
        FeatureCorrelationRow other = (FeatureCorrelationRow) obj;
        if (coefficientType == null) {
            if (other.coefficientType != null) {
                return false;
            }
        } else if (!coefficientType.equals(other.coefficientType)) {
            return false;
        }
        if (featureCorrelationScores == null) {
            if (other.featureCorrelationScores != null) {
                return false;
            }
        } else if (!featureCorrelationScores.equals(other.featureCorrelationScores)) {
            return false;
        }
        if (featureName == null) {
            if (other.featureName != null) {
                return false;
            }
        } else if (!featureName.equals(other.featureName)) {
            return false;
        }
        return true;
    }
    
    @Override
    public String toString() {
        return "FeatureCorrelationRow [featureName=" + featureName + ", coefficientType=" + coefficientType
                + ", featureCorrelationScores=" + featureCorrelationScores + "]";
    }
    
}
