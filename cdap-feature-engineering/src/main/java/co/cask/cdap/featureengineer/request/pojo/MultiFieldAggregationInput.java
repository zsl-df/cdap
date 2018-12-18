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
public class MultiFieldAggregationInput {

    private SchemaColumn destinationColumn;
    private SchemaColumn groupByColumn;
    private List<SchemaColumn> sourceColumns;

    public SchemaColumn getDestinationColumn() {
        return destinationColumn;
    }

    public void setDestinationColumn(SchemaColumn destinationColumn) {
        this.destinationColumn = destinationColumn;
    }

    public List<SchemaColumn> getSourceColumns() {
        return sourceColumns;
    }

    public void setSourceColumns(List<SchemaColumn> sourceColumns) {
        this.sourceColumns = sourceColumns;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((destinationColumn == null) ? 0 : destinationColumn.hashCode());
        result = prime * result + ((sourceColumns == null) ? 0 : sourceColumns.hashCode());
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
        MultiFieldAggregationInput other = (MultiFieldAggregationInput) obj;
        if (destinationColumn == null) {
            if (other.destinationColumn != null) {
                return false;
            }
        } else if (!destinationColumn.equals(other.destinationColumn)) {
            return false;
        }
        if (sourceColumns == null) {
            if (other.sourceColumns != null) {
                return false;
            }
        } else if (!sourceColumns.equals(other.sourceColumns)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "MultiFieldAggregationInput [destinationColumn=" + destinationColumn + ", sourceColumns=" + sourceColumns
                + "]";
    }

    public SchemaColumn getGroupByColumn() {
        return groupByColumn;
    }

    public void setGroupByColumn(SchemaColumn groupByColumn) {
        this.groupByColumn = groupByColumn;
    }

}
