package com.hygenics.parser;

import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import mjson.Json;
import com.eclipsesource.json.JsonObject;

import org.springframework.stereotype.Repository;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * The Properties class is pre-instantiated. It stores any properties to
 * be used by the program. Properties are used by writing poperties: 
 * prior to any data in string form or may be accessed if needed.
 * 
 * They are stored in a Map[String,String]
 * 
 * @author aevans
 *
 */
@Repository
public class Properties {	
	private static Map<String,String> props;
	
	@Autowired
	private getDAOTemplate template;
	
	/**
	 * @return		The properties Map[String,String]
	 */
	public Map<String, String> getProps() {
		return props;
	}
	
	/**
	 * Set the properties from a Map[String,String] of key, value pairs.
	 * 
	 * Special parameters (never add the following prefixes unless planning to use them):
	 * 		-To run a SQL query, prefix the key with SQL:. The value is used as the query.
	 * 
	 * @param props		The Map[String,String] of the properties.
	 */
	public void setProps(Map<String, String> props) {
		
		this.props = props;
		
		if(props != null){
			for(String k : props.keySet()){
				
				if(!k.toLowerCase().startsWith("sql:")){
					System.setProperty(k, props.get(k));
				}else{
					Map<String, Json> jmap = Json.read(getSQLProperty(props.get(k))).asJsonMap();
					String jk = jmap.keySet().iterator().next();
					System.setProperty(k.trim().substring(4).trim(),jmap.get(jk).asString());
				}
			}
		}
		
	}
	
	/**
	 * Get a property from a key string
	 * @param 		k		They key containing.
	 * @return	The string with the key replaced.
	 */
	public static String getProperty(String k){
		String rval = k;
		if(k != null && props != null){
			if(k.contains("property:")){
				Pattern p= Pattern.compile("property:[^\\s]+");
				Matcher m = p.matcher(k);
				
				while(m.find()){
					rval = rval.replace(m.group(0), System.getProperty(m.group(0).replace("property:", "")));
				}
				return rval; 
			}
		}
		return rval;
	}//get a system is a property when wrapped with PROP()
	
	private String getSQLProperty(String q){
		ArrayList<String> data = this.template.getAll(q);
		if(data != null && data.size() > 0){
			JsonObject.readFrom(data.get(0));
			return data.get(0);
		}
		return null;
	}//return a property from a SQL request when a property value is wrapped with SQL()
	
	
}
