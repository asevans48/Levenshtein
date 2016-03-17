package com.hygenics.parser;

import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * Creates tables. Useful for creating Pentaho Tables in a drop, recreate schema
 * situation.
 * 
 * @author asevans
 *
 */
public class CreateTable {

	private Logger log = LoggerFactory.getLogger(MainApp.class);
	private Map<String, List<String>> tables;
	private getDAOTemplate template;

	public CreateTable() {

	}

	public Map<String, List<String>> getTables() {
		return tables;
	}

	@Required
	public void setTables(Map<String, List<String>> tables) {
		this.tables = tables;
	}

	public getDAOTemplate getTemplate() {
		return template;
	}

	@Required
	@Resource(name = "getDAOTemplate")
	public void setTemplate(getDAOTemplate template) {
		this.template = template;
	}

	/**
	 * Private method that creates the tables
	 */
	private void createTables() {

		String query;
		String attrs = null;
		for (String table : tables.keySet()) {
			String[] spl = table.split("\\.");
			log.info("Creating Table " + spl[1] + " ON SCHEMA " + spl[0]);
			if (template.checkTable(table, spl[0]) == false) {

				if (tables.get(table).size() > 0) {
					query = "CREATE TABLE " + table + "(";

					for (String attr : tables.get(table)) {
						attrs = (attrs == null) ? attr
								+ " text, id serial PRIMARY KEY NOT NULL"
								: attrs + "," + attr + " text";
					}

					query += attrs + ");";

					template.execute(query);
					query = null;
					attrs = null;
				} else {
					log.info("No Attributes!\n Cannot Create Table " + table
							+ "!");
				}
			}
		}
	}

	/**
	 * Public method to run the creation of tables
	 */
	public void run() {
		if (tables.size() > 0) {
			createTables();
		} else {
			log.warn("No Tables Specified!");
		}
	}
}
