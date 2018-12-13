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

import javax.naming.OperationNotSupportedException;

/**
 * @author bhupesh.goel
 *
 */
public class StatsFilterRangeLimit {

    private Object lowerLimit;
    private Object upperLimit;

    public StatsFilterRangeLimit(final Object lowerLimit, final Object upperLimit) {
        this.lowerLimit = lowerLimit;
        this.upperLimit = upperLimit;
    }

    public StatsFilterRangeLimit() {
    }

    /**
     * @param filterType
     *            the filterType to set
     * @throws OperationNotSupportedException
     */
    public void setFilterType(StatsFilterType filterType) {
        throw new RuntimeException(
                "StatsFilterRangeLimit doesn't support setting filter type. By Default it will always be Range");
    }

    /**
     * @return the lowerLimit
     */
    public Object getLowerLimit() {
        return lowerLimit;
    }

    /**
     * @param lowerLimit
     *            the lowerLimit to set
     */
    public void setLowerLimit(Object lowerLimit) {
        this.lowerLimit = lowerLimit;
    }

    /**
     * @return the upperLimit
     */
    public Object getUpperLimit() {
        return upperLimit;
    }

    /**
     * @param upperLimit
     *            the upperLimit to set
     */
    public void setUpperLimit(Object upperLimit) {
        this.upperLimit = upperLimit;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((lowerLimit == null) ? 0 : lowerLimit.hashCode());
        result = prime * result + ((upperLimit == null) ? 0 : upperLimit.hashCode());
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
        StatsFilterRangeLimit other = (StatsFilterRangeLimit) obj;
        if (lowerLimit == null) {
            if (other.lowerLimit != null) {
                return false;
            }
        } else if (!lowerLimit.equals(other.lowerLimit)) {
            return false;
        }
        if (upperLimit == null) {
            if (other.upperLimit != null) {
                return false;
            }
        } else if (!upperLimit.equals(other.upperLimit)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "StatsFilterRangeLimit [lowerLimit=" + lowerLimit + ", upperLimit=" + upperLimit + "]";
    }

}
