/*
 --------------------------------------------------------------------------------
 SPADE - Support for Provenance Auditing in Distributed Environments.
 Copyright (C) 2012 SRI International

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

package spade.utility;

import java.io.Serializable;

/**
 *
 * A generic interface specifically added to be used by ExternalMemoryMap class 
 * to get a custom hashcode of type T instead of the one gotten by default hashCode()
 * function 
 * 
 * @param <T> Object type to get the hashcode of
 *
 */

public interface Hasher<T> extends Serializable{
	public String getHash(T t);
}