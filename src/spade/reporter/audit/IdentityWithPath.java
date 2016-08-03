/*
 --------------------------------------------------------------------------------
 SPADE - Support for Provenance Auditing in Distributed Environments.
 Copyright (C) 2015 SRI International

 This program is free software: you can redistribute it and/or
 modify it under the terms of the GNU General Public License as
 published by the Free Software Foundation, either version 3 of the
 License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program. If not, see <http://www.gnu.org/licenses/>.
 --------------------------------------------------------------------------------
 */

package spade.reporter.audit;

import java.util.HashMap;
import java.util.Map;

/**
 * Just a convenience class. Added because FileIdentity, NamedPipeIdentity and UnixSocketIdentity all have the same implementation.
 * So they all extend this class.
 */
public abstract class IdentityWithPath implements ArtifactIdentity{
	
	private static final long serialVersionUID = -6225520209209163585L;
	
	private String path;
	
	public IdentityWithPath(String path){
		path = path.replace("//", "/");
		this.path = path;
	}
	
	@Override
	public Map<String, String> getAnnotationsMap(){
		Map<String, String> annotations = new HashMap<String, String>();
		annotations.put("path", path);
		return annotations;
	}
	
	public String getPath(){
		return path;
	}
	
	public abstract String getSubtype();
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((path == null) ? 0 : path.hashCode());
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
		IdentityWithPath other = (IdentityWithPath) obj;
		if (path == null) {
			if (other.path != null)
				return false;
		} else if (!path.equals(other.path))
			return false;
		return true;
	}
}
