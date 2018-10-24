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
public class ColumnDictionary {

	private SchemaColumn column;
	private List<String> dictionary;

	public SchemaColumn getColumn() {
		return column;
	}

	public void setColumn(SchemaColumn column) {
		this.column = column;
	}

	public List<String> getDictionary() {
		return dictionary;
	}

	public void setDictionary(List<String> dictionary) {
		this.dictionary = dictionary;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((column == null) ? 0 : column.hashCode());
		result = prime * result + ((dictionary == null) ? 0 : dictionary.hashCode());
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
		ColumnDictionary other = (ColumnDictionary) obj;
		if (column == null) {
			if (other.column != null)
				return false;
		} else if (!column.equals(other.column))
			return false;
		if (dictionary == null) {
			if (other.dictionary != null)
				return false;
		} else if (!dictionary.equals(other.dictionary))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "ColumnDictionary [column=" + column + ", dictionary=" + dictionary + "]";
	}

}
