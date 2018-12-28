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

import co.cask.cdap.featureengineer.proto.FeatureGenerationRequest;

/**
 * @author bhupesh.goel
 *
 */
public class PipelineGenerationInfo {
    
    private FeatureGenerationRequest featureGenerationRequest;
    private String pipelineStatus;
    private Long lastStartEpochTime;
    private String lastRunId;
    
    public PipelineGenerationInfo(FeatureGenerationRequest request, String pipelineStatus, Long lastStartEpochTime,
            String lastRunId) {
        this.featureGenerationRequest = request;
        this.pipelineStatus = pipelineStatus;
        this.lastStartEpochTime = lastStartEpochTime;
        this.lastRunId = lastRunId;
    }
    
    /**
     * 
     */
    public PipelineGenerationInfo() {
        
    }
    
    /**
     * @return the featureGenerationRequest
     */
    public FeatureGenerationRequest getFeatureGenerationRequest() {
        return featureGenerationRequest;
    }
    
    /**
     * @param featureGenerationRequest
     *            the featureGenerationRequest to set
     */
    public void setFeatureGenerationRequest(FeatureGenerationRequest featureGenerationRequest) {
        this.featureGenerationRequest = featureGenerationRequest;
    }
    
    /**
     * @return the pipelineStatus
     */
    public String getPipelineStatus() {
        return pipelineStatus;
    }
    
    /**
     * @param pipelineStatus
     *            the pipelineStatus to set
     */
    public void setPipelineStatus(String pipelineStatus) {
        this.pipelineStatus = pipelineStatus;
    }
    
    /**
     * @return the lastStartEpochTime
     */
    public Long getLastStartEpochTime() {
        return lastStartEpochTime;
    }
    
    /**
     * @param lastStartEpochTime
     *            the lastStartEpochTime to set
     */
    public void setLastStartEpochTime(Long lastStartEpochTime) {
        this.lastStartEpochTime = lastStartEpochTime;
    }
    
    /**
     * @return the lastRunId
     */
    public String getLastRunId() {
        return lastRunId;
    }
    
    /**
     * @param lastRunId
     *            the lastRunId to set
     */
    public void setLastRunId(String lastRunId) {
        this.lastRunId = lastRunId;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((featureGenerationRequest == null) ? 0 : featureGenerationRequest.hashCode());
        result = prime * result + ((lastRunId == null) ? 0 : lastRunId.hashCode());
        result = prime * result + ((lastStartEpochTime == null) ? 0 : lastStartEpochTime.hashCode());
        result = prime * result + ((pipelineStatus == null) ? 0 : pipelineStatus.hashCode());
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
        PipelineGenerationInfo other = (PipelineGenerationInfo) obj;
        if (featureGenerationRequest == null) {
            if (other.featureGenerationRequest != null) {
                return false;
            }
        } else if (!featureGenerationRequest.equals(other.featureGenerationRequest)) {
            return false;
        }
        if (lastRunId == null) {
            if (other.lastRunId != null) {
                return false;
            }
        } else if (!lastRunId.equals(other.lastRunId)) {
            return false;
        }
        if (lastStartEpochTime == null) {
            if (other.lastStartEpochTime != null) {
                return false;
            }
        } else if (!lastStartEpochTime.equals(other.lastStartEpochTime)) {
            return false;
        }
        if (pipelineStatus == null) {
            if (other.pipelineStatus != null) {
                return false;
            }
        } else if (!pipelineStatus.equals(other.pipelineStatus)) {
            return false;
        }
        return true;
    }
    
    @Override
    public String toString() {
        return "PipelineGenerationInfo [featureGenerationRequest=" + featureGenerationRequest + ", pipelineStatus="
                + pipelineStatus + ", lastStartEpochTime=" + lastStartEpochTime + ", lastRunId=" + lastRunId + "]";
    }
    
}
