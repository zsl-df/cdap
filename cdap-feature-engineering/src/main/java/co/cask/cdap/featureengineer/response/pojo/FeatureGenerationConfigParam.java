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
public class FeatureGenerationConfigParam {
    
    private String paramName;
    private String description;
    private boolean isCollection;
    private String dataType;
    private boolean isMandatory;
    private List<FeatureGenerationConfigParam> subParams;
    
    public FeatureGenerationConfigParam(final String paramName, final String description, final String dataType,
            final boolean isCollection, final boolean isMandatory) {
        this.paramName = paramName;
        this.description = description;
        this.isCollection = isCollection;
        this.dataType = dataType;
        this.isMandatory = isMandatory;
    }
    
    public void addSubParam(FeatureGenerationConfigParam param) {
        if (this.subParams == null) {
            this.subParams = new LinkedList<>();
        }
        this.subParams.add(param);
    }
    
    /**
     * @return the paramName
     */
    public String getParamName() {
        return paramName;
    }
    
    /**
     * @param paramName
     *            the paramName to set
     */
    public void setParamName(String paramName) {
        this.paramName = paramName;
    }
    
    /**
     * @return the description
     */
    public String getDescription() {
        return description;
    }
    
    /**
     * @param description
     *            the description to set
     */
    public void setDescription(String description) {
        this.description = description;
    }
    
    /**
     * @return the isCollection
     */
    public boolean isCollection() {
        return isCollection;
    }
    
    /**
     * @param isCollection
     *            the isCollection to set
     */
    public void setCollection(boolean isCollection) {
        this.isCollection = isCollection;
    }
    
    /**
     * @return the dataType
     */
    public String getDataType() {
        return dataType;
    }
    
    /**
     * @param dataType
     *            the dataType to set
     */
    public void setDataType(String dataType) {
        this.dataType = dataType;
    }
    
    /**
     * @return the isMandatory
     */
    public boolean isMandatory() {
        return isMandatory;
    }
    
    /**
     * @param isMandatory
     *            the isMandatory to set
     */
    public void setMandatory(boolean isMandatory) {
        this.isMandatory = isMandatory;
    }
    
    /**
     * @return the subParams
     */
    public List<FeatureGenerationConfigParam> getSubParams() {
        return subParams;
    }
    
    /**
     * @param subParams
     *            the subParams to set
     */
    public void setSubParams(List<FeatureGenerationConfigParam> subParams) {
        this.subParams = subParams;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dataType == null) ? 0 : dataType.hashCode());
        result = prime * result + ((description == null) ? 0 : description.hashCode());
        result = prime * result + (isCollection ? 1231 : 1237);
        result = prime * result + (isMandatory ? 1231 : 1237);
        result = prime * result + ((paramName == null) ? 0 : paramName.hashCode());
        result = prime * result + ((subParams == null) ? 0 : subParams.hashCode());
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
        FeatureGenerationConfigParam other = (FeatureGenerationConfigParam) obj;
        if (dataType == null) {
            if (other.dataType != null) {
                return false;
            }
        } else if (!dataType.equals(other.dataType)) {
            return false;
        }
        if (description == null) {
            if (other.description != null) {
                return false;
            }
        } else if (!description.equals(other.description)) {
            return false;
        }
        if (isCollection != other.isCollection) {
            return false;
        }
        if (isMandatory != other.isMandatory) {
            return false;
        }
        if (paramName == null) {
            if (other.paramName != null) {
                return false;
            }
        } else if (!paramName.equals(other.paramName)) {
            return false;
        }
        if (subParams == null) {
            if (other.subParams != null) {
                return false;
            }
        } else if (!subParams.equals(other.subParams)) {
            return false;
        }
        return true;
    }
    
    @Override
    public String toString() {
        return "FeatureGenerationConfigParam [paramName=" + paramName + ", description=" + description
                + ", isCollection=" + isCollection + ", dataType=" + dataType + ", isMandatory=" + isMandatory
                + ", subParams=" + subParams + "]";
    }
    
}
