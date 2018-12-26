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
public class FeatureVIFIterationRow {
    
    private String featureName;
    private double vifScore;
    private Map<String, Double> allVIFScores;
    
    public FeatureVIFIterationRow() {
        this.allVIFScores = new HashMap<>();
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
     * @return the vifScore
     */
    public double getVifScore() {
        return vifScore;
    }
    
    /**
     * @param vifScore
     *            the vifScore to set
     */
    public void setVifScore(double vifScore) {
        this.vifScore = vifScore;
    }
    
    /**
     * @return the allVIFScores
     */
    public Map<String, Double> getAllVIFScores() {
        return allVIFScores;
    }
    
    /**
     * @param allVIFScores
     *            the allVIFScores to set
     */
    public void setAllVIFScores(Map<String, Double> allVIFScores) {
        this.allVIFScores = allVIFScores;
    }
    
    /**
     * 
     * @param feature
     * @param vifScore
     */
    public void addVIFScore(String feature, Double vifScore) {
        this.allVIFScores.put(feature, vifScore);
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((allVIFScores == null) ? 0 : allVIFScores.hashCode());
        result = prime * result + ((featureName == null) ? 0 : featureName.hashCode());
        long temp;
        temp = Double.doubleToLongBits(vifScore);
        result = prime * result + (int) (temp ^ (temp >>> 32));
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
        FeatureVIFIterationRow other = (FeatureVIFIterationRow) obj;
        if (allVIFScores == null) {
            if (other.allVIFScores != null) {
                return false;
            }
        } else if (!allVIFScores.equals(other.allVIFScores)) {
            return false;
        }
        if (featureName == null) {
            if (other.featureName != null) {
                return false;
            }
        } else if (!featureName.equals(other.featureName)) {
            return false;
        }
        if (Double.doubleToLongBits(vifScore) != Double.doubleToLongBits(other.vifScore)) {
            return false;
        }
        return true;
    }
    
    @Override
    public String toString() {
        return "FeatureVIFIterationRow [featureName=" + featureName + ", vifScore=" + vifScore + ", allVIFScores="
                + allVIFScores + "]";
    }
    
}
