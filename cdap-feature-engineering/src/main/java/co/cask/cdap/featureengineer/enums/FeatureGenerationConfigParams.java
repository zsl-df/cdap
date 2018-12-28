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
package co.cask.cdap.featureengineer.enums;

/**
 * @author bhupesh.goel
 *
 */
public enum FeatureGenerationConfigParams {

    INDEXES("Indexes", true, true, "unknown", "", "", false),
    RELATIONSHIPS("Relationships", true, true, "unknown", "", "Column1,Column2", false),
    TIMESTAMP_COLUMNS("TimestampColumns", true, true, "unknown", "", "", false),
    CREATE_ENTITIES("CreateEntities", true, true, "unknown", "", "", false),
    TIME_INDEX_COLUMNS("TimeIndexColumns", true, true, "unknown", "", "", false),
    CATEGORICAL_COLUMNS("CategoricalColumns", true, true, "unknown", "", "", false), 
    IGNORE_COLUMNS("IgnoreColumns", true, true, "unknown", "", "", false),
    MULTI_FIELD_TRANS_FUNCTION_INPUT_COLUMNS("multiFieldTransFunctionInputColumns", true, true, "unknown", "", "", 
            false),  
    MULTI_FIELD_AGG_FUNCTION_INPUT_COLUMNS("multiFieldAggFunctionInputColumns", true, true, "unknown", "", 
            "DestinationColumn,GroupByColumn,SourceColumns", false), 
    TARGET_ENTITY("TargetEntity", true, false, "unknown", "", "", false), 
    TARGET_ENTITY_PRIMARY_FIELD("TargetEntityPrimaryField", true, false, "unknown", "", "", false),  
    DFS_DEPTH("DFSDepth", false, false, "int", "", "", false), 
    TRAINING_WINDOWS("TrainingWindows", false, true, "int", "", "", false),  
    WINDOW_END_TIME("WindowEndTime", false, false, "string", "", "", false),
    COLUMN_ONE("Column1", true, false, "unknown", "", "", true),
    COLUMN_TWO("Column2", true, false, "unknown", "", "", true),
    DESTINATION_COLUMN("DestinationColumn", true, false, "unknown", "", "", true),
    GROUP_BY_COLUMN("GroupByColumn", true, false, "unknown", "", "", true),
    SOURCE_COLUMNS("SourceColumns", true, true, "unknown", "", "", true);

    final String name;
    final Boolean isSchemaSpecific;
    final Boolean isCollection;
    final String dataType;
    final String description;
    final String subParams;
    final Boolean isSubParam;
    
    FeatureGenerationConfigParams(final String name, final boolean isSchemaSpecific, final Boolean isCollection,
            final String dataType, final String description, final String subParams, final Boolean isSubParam) {
        this.name = name;
        this.isSchemaSpecific = isSchemaSpecific;
        this.isCollection = isCollection;
        this.dataType = dataType;
        this.description = description;
        this.subParams = subParams;
        this.isSubParam = isSubParam;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the isSchemaSpecific
     */
    public Boolean getIsSchemaSpecific() {
        return isSchemaSpecific;
    }

    /**
     * @return the isCollection
     */
    public Boolean getIsCollection() {
        return isCollection;
    }

    /**
     * @return the dataType
     */
    public String getDataType() {
        return dataType;
    }

    /**
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * @return the subParams
     */
    public String getSubParams() {
        return subParams;
    }

    /**
     * @return the isSubParam
     */
    public Boolean getIsSubParam() {
        return isSubParam;
    }


}
