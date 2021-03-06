/*
 --------------------------------------------------------------------------------
 SPADE - Support for Provenance Auditing in Distributed Environments.
 Copyright (C) 2016 SRI International

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
package spade.reporter;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import spade.core.AbstractEdge;
import spade.core.AbstractReporter;
import spade.core.AbstractVertex;
import spade.core.Settings;
import spade.utility.FileUtility;
import spade.edge.prov.ActedOnBehalfOf;
import spade.edge.prov.Used;
import spade.edge.prov.WasAssociatedWith;
import spade.edge.prov.WasAttributedTo;
import spade.edge.prov.WasDerivedFrom;
import spade.edge.prov.WasGeneratedBy;
import spade.edge.prov.WasInformedBy;
import spade.vertex.prov.Activity;
import spade.vertex.prov.Agent;
import spade.vertex.prov.Entity;
import java.util.Map;

/**
 * CamFlow reporter for SPADE
 *
 * @author Aurélien Chaline from the original file JSON.java by Hasanat Kazmi
 */
public class CamFlow extends AbstractReporter {

    private boolean shutdown = false;
    private boolean PRINT_DEBUG = true;
    private HashMap<String, Object[] > vertices;
    int batchSize = 5;
    private boolean missing = false;
    private ArrayList<JSONObject> missingEdge = new ArrayList<JSONObject>();
    @Override
    public boolean launch(final String arguments) {
        /*
        * argument is path to CamFlow output
        */
        vertices = new HashMap<String, Object[]>();
	
        	
	Map<String, String> configMap = readDefaultConfigFile();//gets the batch size from the config file
	if(configMap != null){
		this.batchSize = Integer.parseInt(configMap.get("batch_size"));

	}

        Runnable eventThread = new Runnable() {
            public void run() {

                String file_path = arguments.trim();
                String line;
                String jsonString = "";
                BufferedReader br;
                JSONArray jsonArray;
		int count = 0;
                try {
                  debugLog("Starting to read CamFlow output");
                  br = new BufferedReader(new InputStreamReader(new FileInputStream(file_path)));
		  line = br.readLine();
		  while (line.charAt(0) != '['){line = br.readLine();}
		  br.readLine();br.readLine();//removes thrre first useless lines
                  while ((line = br.readLine()) != null) {
		      JSONObject[] buf = new JSONObject[batchSize];
		      for(int i =0; i < batchSize; i++){
			if(i != 0) br.readLine();
			if((line = br.readLine()) == null) break;
                      	jsonString += ("{"+line);
		      	jsonString += br.readLine();
		      	jsonString += br.readLine()+"}";
		      	JSONObject json = new JSONObject(jsonString);
		      	buf[i] = json;
			jsonString = "";
			count++;
			br.readLine();
		      }
		      for(int i = 0; i < batchSize && buf[i] != null ; i++){
		      	processJsonString(buf[i]);
		      }
		      jsonString = "";
		      if (count % 1000 == 0) MissingEdgesAdd();
		      
		  }  
		}
		catch(Exception e){CamFlow.log(Level.SEVERE, "Unknown object type: problem reading", null);}
 	        missing = true;
		MissingEdgesAdd();
		debugLog("Job is over, processed "+ count + " lines.");
              }
        };
        new Thread(eventThread, "CamFlowReporter-Thread").start();
        return true;
    }

    @Override
    public boolean shutdown() {
        shutdown=true;
        return true;
    }

    private void MissingEdgesAdd(){
 	    for(int i = 0; i < missingEdge.size(); i++){
 		processJsonString(missingEdge.get(i));
 	    }
     }
    private Map<String, String> readDefaultConfigFile(){
                try{
                        return FileUtility.readConfigFileAsKeyValueMap(
                                        Settings.getDefaultConfigFilePath(this.getClass()),
                                        "="
                                        );
                }catch(Exception e){
                        CamFlow.log(Level.SEVERE, "Failed to load config file", e);
                        return null;
                }
    }

    private void processJsonString(JSONObject jsonObject) {
        String objectType;
        try {
          objectType = jsonObject.getString("type");
        

        if (objectType.equalsIgnoreCase("Activity") ||
          objectType.equalsIgnoreCase("Agent") ||
          objectType.equalsIgnoreCase("Entity")
        ) {
          processVertex(jsonObject);
        } else if (objectType.equalsIgnoreCase("ActedOnBehalfOf") ||
          objectType.equalsIgnoreCase("Used") ||
          objectType.equalsIgnoreCase("WasAssociatedWith") ||
          objectType.equalsIgnoreCase("WasAttributedTo") ||
          objectType.equalsIgnoreCase("WasDerivedFrom") ||
          objectType.equalsIgnoreCase("WasGeneratedBy") ||
          objectType.equalsIgnoreCase("WasInformedBy")
        ){
          processEdge(jsonObject);
        } else {
          CamFlow.log(Level.SEVERE, "Unknown object type: '" + objectType + "', ignoring object", null);
        }
	
        } catch (JSONException e) {
          CamFlow.log(Level.SEVERE, "Missing type in object, can not access if its node or edge, ignoring object", null);
	}
    }

    private void processVertex(JSONObject vertexObject) {
      // Activity, Agent, Entity
      String id = null;
      try {
    	Object idValue = vertexObject.get("id");
    	if(idValue == null){
    		throw new JSONException("");
    	}
        id = String.valueOf(idValue);
      } catch (JSONException e) {
        CamFlow.log(Level.SEVERE, "Missing id in vertex, ignoring vertex : " + vertexObject.toString() , null);
        return;
      }
      String vertexType;
      try {
        vertexType = vertexObject.getString("type");
      } catch (JSONException e) {
        // this is already been checked
        return;
      }

      AbstractVertex vertex = null;
      if (vertexType.equalsIgnoreCase("Activity")) {
        vertex = new Activity();
      } else if (vertexType.equalsIgnoreCase("Agent")) {
        vertex = new Agent();
      } else if (vertexType.equalsIgnoreCase("Entity")) {
        vertex = new Entity();
      }

      JSONObject annotationsObject;
      try {
        annotationsObject = vertexObject.getJSONObject("annotations");
        if (annotationsObject.length()!=0) {
          for(int i = 0; i<annotationsObject.names().length(); i++){
               vertex.addAnnotation(annotationsObject.names().getString(i), annotationsObject.get(annotationsObject.names().getString(i)).toString());
           }
          }
        }
      catch (JSONException e) {
        // no annotations
      }

      addVertex(id, vertex);//ads the vertex to the Hashmap
      putVertex(vertex);
    }

    private void processEdge(JSONObject edgeObject) {
      String from;
      try {
        Object fromValue = edgeObject.get("from");
        if(fromValue == null){
        	throw new JSONException("");
        }
        from = String.valueOf(fromValue);
      } catch (JSONException e) {
        CamFlow.log(Level.SEVERE, "Missing 'from' in edge, ignoring edge : " + edgeObject.toString() , null);
        return;
      }

      String to;
      try {
        Object toValue = edgeObject.get("to");
        if(toValue == null){
        	throw new JSONException("");
        }
        to = String.valueOf(toValue);
      } catch (JSONException e) {
        CamFlow.log(Level.SEVERE, "Missing 'to' in edge, ignoring edge : " + edgeObject.toString() , null);
        return;
      }

      AbstractVertex fromVertex = getVertex(from);
      AbstractVertex toVertex = getVertex(to);

      if (fromVertex == null || toVertex == null) {
	      try{
        	if(missing) CamFlow.log(Level.SEVERE, "Starting and/or ending vertex of edge hasn't been seen before, ignoring edge : " + edgeObject, null);
	      }
	      catch(Exception e){}
	      if (!missing)missingEdge.add(edgeObject);
	      if (fromVertex != null) addVertex(from,fromVertex);
	      if (toVertex   != null) addVertex(to  ,toVertex  );
	return;
      }

      String edgeType;
      try {
        edgeType = edgeObject.getString("type");
      } catch (JSONException e) {
        // this is already been checked
        return;
      }

      AbstractEdge edge = null;
      if (edgeType.equalsIgnoreCase("ActedOnBehalfOf")) {
        edge = new ActedOnBehalfOf((Agent) fromVertex, (Agent) toVertex);
      } else if (edgeType.equalsIgnoreCase("WasAttributedTo")) {
        edge = new WasAttributedTo((Entity) fromVertex, (Agent) toVertex);
      } else if (edgeType.equalsIgnoreCase("WasDerivedFrom")) {
        edge = new WasDerivedFrom((Entity) fromVertex, (Entity) toVertex);
      } else if (edgeType.equalsIgnoreCase("WasGeneratedBy")) {
        edge = new WasGeneratedBy((Entity) fromVertex, (Activity) toVertex);
      } else if (edgeType.equalsIgnoreCase("WasInformedBy")) {
        edge = new WasInformedBy((Activity) fromVertex, (Activity) toVertex);
      } else if (edgeType.equalsIgnoreCase("Used")) {
        edge = new Used((Activity) fromVertex, (Entity) toVertex);
      } else if (edgeType.equalsIgnoreCase("WasAssociatedWith")) {
        edge = new WasAssociatedWith((Activity) fromVertex, (Agent) toVertex);
      }

      JSONObject annotationsObject;
     
	try {
        annotationsObject = edgeObject.getJSONObject("annotations");
        if (annotationsObject.length()!=0) {
          for(int i = 0; i<annotationsObject.names().length(); i++){
               edge.addAnnotation(annotationsObject.names().getString(i), annotationsObject.get(annotationsObject.names().getString(i)).toString());
                }
           }
        

      } catch (JSONException e) {
        // no annotations
      }

      putEdge(edge);
    }

    public void debugLog(String msg) {
      if (PRINT_DEBUG == true) {
        CamFlow.log(Level.INFO, msg, null);
      }
    }

    public static void log(Level level, String msg, Throwable thrown) {
        if (level == level.FINE) {
        } else {
            Logger.getLogger(CamFlow.class.getName()).log(level, msg, thrown);
        }
    }

    private void addVertex(String edgeId, AbstractVertex edge){
    	if (vertices.containsKey(edgeId)) {
		Object[] arr  = vertices.get(edgeId);
		vertices.put(edgeId, new Object[] {edge,(Integer) arr[1] + 1} );
	}
	else {
		vertices.put(edgeId, new Object[] {edge, 1});
	}
    }

    private AbstractVertex getVertex(String edgeId) {
	Object[] arr = vertices.get(edgeId);
	if (arr == null) return null;
	AbstractVertex edge = (AbstractVertex) arr[0]; // gets the edge, stored first

	if( (Integer) arr[1] == 1 ) {//if there was only one entry associated to this edge, remove from the hashmap
		vertices.remove(edgeId);
	}
	else {
		vertices.put(edgeId, new Object[] {edge,(Integer) arr[1] - 1} );//else we just replace the entry
	}
	return edge;
    }
    
}
