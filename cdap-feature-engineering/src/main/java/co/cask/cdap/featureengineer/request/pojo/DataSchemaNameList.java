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
package co.cask.cdap.featureengineer.request.pojo;

import java.util.List;

/**
 * @author bhupesh.goel
 *
 */
public class DataSchemaNameList {

    List<String> dataSchemaName;

    /**
     * @return the dataSchemaName
     */
    public List<String> getDataSchemaName() {
        return dataSchemaName;
    }

    /**
     * @param dataSchemaName
     *            the dataSchemaName to set
     */
    public void setDataSchemaName(List<String> dataSchemaName) {
        this.dataSchemaName = dataSchemaName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dataSchemaName == null) ? 0 : dataSchemaName.hashCode());
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
        DataSchemaNameList other = (DataSchemaNameList) obj;
        if (dataSchemaName == null) {
            if (other.dataSchemaName != null) {
                return false;
            }
        } else if (!dataSchemaName.equals(other.dataSchemaName)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "DataSchemaNameList [dataSchemaName=" + dataSchemaName + "]";
    }

}
