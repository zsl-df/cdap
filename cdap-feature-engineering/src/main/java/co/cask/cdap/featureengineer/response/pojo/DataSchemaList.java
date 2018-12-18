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
public class DataSchemaList {

    private List<DataSchema> dataSchemaList;

    /**
     * @return the dataSchemaList
     */
    public List<DataSchema> getDataSchemaList() {
        return dataSchemaList;
    }

    /**
     * @param dataSchemaList
     *            the dataSchemaList to set
     */
    public void setDataSchemaList(List<DataSchema> dataSchemaList) {
        this.dataSchemaList = dataSchemaList;
    }

    public void addDataSchema(DataSchema schema) {
        if (dataSchemaList == null) {
            this.dataSchemaList = new LinkedList<>();
        }
        this.dataSchemaList.add(schema);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dataSchemaList == null) ? 0 : dataSchemaList.hashCode());
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
        DataSchemaList other = (DataSchemaList) obj;
        if (dataSchemaList == null) {
            if (other.dataSchemaList != null) {
                return false;
            }
        } else if (!dataSchemaList.equals(other.dataSchemaList)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "DataSchemaList [dataSchemaList=" + dataSchemaList + "]";
    }

}
