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

import co.cask.cdap.featureengineer.request.pojo.Column;

import java.util.LinkedList;
import java.util.List;

/**
 * @author bhupesh.goel
 *
 */
public class DataSchema {

    private String schemaName;
    private List<Column> schemaColumns;

    /**
     * @return the schemaName
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * @param schemaName
     *            the schemaName to set
     */
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @return the schemaColumns
     */
    public List<Column> getSchemaColumns() {
        return schemaColumns;
    }

    /**
     * @param schemaColumns
     *            the schemaColumns to set
     */
    public void setSchemaColumns(List<Column> schemaColumns) {
        this.schemaColumns = schemaColumns;
    }

    public void addSchemaColumn(Column column) {
        if (schemaColumns == null) {
            schemaColumns = new LinkedList<>();
        }
        schemaColumns.add(column);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((schemaColumns == null) ? 0 : schemaColumns.hashCode());
        result = prime * result + ((schemaName == null) ? 0 : schemaName.hashCode());
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
        DataSchema other = (DataSchema) obj;
        if (schemaColumns == null) {
            if (other.schemaColumns != null) {
                return false;
            }
        } else if (!schemaColumns.equals(other.schemaColumns)) {
            return false;
        }
        if (schemaName == null) {
            if (other.schemaName != null) {
                return false;
            }
        } else if (!schemaName.equals(other.schemaName)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "DataSchema [schemaName=" + schemaName + ", schemaColumns=" + schemaColumns + "]";
    }

}
