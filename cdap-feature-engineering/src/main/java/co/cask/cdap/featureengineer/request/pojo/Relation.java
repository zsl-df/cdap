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

/**
 * @author bhupesh.goel
 *
 */
public class Relation {

	private SchemaColumn column1;
	private SchemaColumn column2;

	public SchemaColumn getColumn1() {
		return column1;
	}

	public void setColumn1(SchemaColumn column1) {
		this.column1 = column1;
	}

	public SchemaColumn getColumn2() {
		return column2;
	}

	public void setColumn2(SchemaColumn column2) {
		this.column2 = column2;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((column1 == null) ? 0 : column1.hashCode());
		result = prime * result + ((column2 == null) ? 0 : column2.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Relation other = (Relation) obj;
		if (column1 == null) {
			if (other.column1 != null)
				return false;
		} else if (!column1.equals(other.column1))
			return false;
		if (column2 == null) {
			if (other.column2 != null)
				return false;
		} else if (!column2.equals(other.column2))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Relation [column1=" + column1 + ", column2=" + column2 + "]";
	}

}
