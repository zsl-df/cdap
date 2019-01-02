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

    INDEXES("Indexes", true, true, true, "unknown", "", "", false),
    RELATIONSHIPS("Relationships", true, true, true, "unknown", "", "Column1,Column2", false),
    TIMESTAMP_COLUMNS("TimestampColumns", false, true, true, "unknown", "", "", false),
    CREATE_ENTITIES("CreateEntities", false, true, true, "unknown", "", "", false),
    TIME_INDEX_COLUMNS("TimeIndexColumns", false, true, true, "unknown", "", "", false),
    CATEGORICAL_COLUMNS("CategoricalColumns", false, true, true, "unknown", "", "", false), 
    IGNORE_COLUMNS("IgnoreColumns", false, true, true, "unknown", "", "", false),
    MULTI_FIELD_TRANS_FUNCTION_INPUT_COLUMNS("multiFieldTransFunctionInputColumns", false, true, true, "unknown", "", 
            "", false),  
    MULTI_FIELD_AGG_FUNCTION_INPUT_COLUMNS("multiFieldAggFunctionInputColumns", false, true, true, "unknown", "", 
            "DestinationColumn,GroupByColumn,SourceColumns", false), 
    TARGET_ENTITY("TargetEntity", true, true, false, "unknown", "", "", false), 
    TARGET_ENTITY_PRIMARY_FIELD("TargetEntityPrimaryField", true, true, false, "unknown", "", "", false),  
    DFS_DEPTH("DFSDepth", true, false, false, "int", "", "", false), 
    TRAINING_WINDOWS("TrainingWindows", false, false, true, "int", "", "", false),  
    WINDOW_END_TIME("WindowEndTime", false, false, false, "string", "", "", false),
    COLUMN_ONE("Column1", true, true, false, "unknown", "", "", true),
    COLUMN_TWO("Column2", true, true, false, "unknown", "", "", true),
    DESTINATION_COLUMN("DestinationColumn", true, true, false, "unknown", "", "", true),
    GROUP_BY_COLUMN("GroupByColumn", true, true, false, "unknown", "", "", true),
    SOURCE_COLUMNS("SourceColumns", true, true, true, "unknown", "", "", true),
    DRIVER_MEMORY("DriverMemoryInMB", false, false, false, "int", "", "", false),
    DRIVER_VIRTUAL_CORES("DriverVirtualCores", false, false, false, "int", "", "", false),
    RESOURCE_MEMORY("ResourceMemoryInMB", false, false, false, "int", "", "", false),
    RESOURCE_VIRTUAL_CORE("ResourceVirtualCore", false, false, false, "int", "", "", false),
    CATEGORICAL_COLUMN_DICTIONARY_LIMIT("CategoricalColumnDictionaryLimit", false, false, false, "int", "", "", false),
    SKIP_HOT_ENCODING("SkipHotEncoding", false, false, false, "boolean", "", "", false),
    LINEAR_REGRESSION_ITERATIONS("LinearRegressionIterations", false, false, false, "int", "", "", false),
    LINEAR_REGRESSION_STEP_SIZE("LinearRegressionStepSize", false, false, false, "double", "", "", false),
    COMPUTE_VIF_SCORES("ComputeVIFScores", false, false, false, "boolean", "", "", false),
    NUM_DATA_PARTITIONS("NumDataPartitions", false, false, false, "int", "", "", false),
    DRIVER_SIDE_JOB_PARALLELISM("DriverSideJobParallelism", false, false, false, "int", "", "", false);

    final String name;
    final Boolean isMandatory;
    final Boolean isSchemaSpecific;
    final Boolean isCollection;
    final String dataType;
    final String description;
    final String subParams;
    final Boolean isSubParam;
    
    FeatureGenerationConfigParams(final String name, final boolean isMandatory, final boolean isSchemaSpecific, 
            final Boolean isCollection, final String dataType, final String description, final String subParams, 
            final Boolean isSubParam) {
        this.name = name;
        this.isSchemaSpecific = isSchemaSpecific;
        this.isCollection = isCollection;
        this.dataType = dataType;
        this.description = description;
        this.subParams = subParams;
        this.isSubParam = isSubParam;
        this.isMandatory = isMandatory;
    }

    /**
     * @return the isMandatory
     */
    public Boolean getIsMandatory() {
        return isMandatory;
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
