package com.hygenics.parser;

import java.util.List;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * Drops a list of tables as needed.
 * 
 * Tables must be in a List<String> and in the format schema.table
 * 
 * @author asevans
 *
 */
public class DropTables {

	private Logger log = LoggerFactory.getLogger(MainApp.class);
	private List<String> tables;
	private getDAOTemplate template;

	public DropTables() {

	}

	/**
	 * Get the Tables to be Dropped
	 * 
	 * @return
	 */
	public List<String> getTables() {
		return tables;
	}

	/**
	 * Set the tables to be dropped. Tables to be dropped must be of the format
	 * schema.table
	 * 
	 * @param tables
	 */
	@Required
	public void setTables(List<String> tables) {
		this.tables = tables;
	}

	public getDAOTemplate getTemplate() {
		return template;
	}

	/**
	 * Auto sets the getDAOTemplate
	 * 
	 * @param template
	 */
	@Required
	@Resource(name = "getDAOTemplate")
	public void setTemplate(getDAOTemplate template) {
		this.template = template;
	}

	/**
	 * Performs the drops
	 */
	private void drop() {
		log.info("Dropping Tables");
		String query;
		String[] split;

		for (String table : tables) {
			split = table.split(".");

			if (split.length == 2) {
				if (template.checkTable(split[0], split[1])) {
					query = "DROP TABLE " + table;
					template.execute(query);
				} else {
					log.warn("Table " + table + " does not exixt!");
				}
			} else {
				log.warn("Innapropriate format " + table
						+ ". Format should be schema.table!");
			}
		}
		log.info("Done Dropping Tables");
	}

	/**
	 * Public method that calls the drop script
	 */
	public void run() {
		log.info("Checking if Tables Exist");
		if (tables != null) {
			drop();
		} else {
			try {
				throw new NullPointerException("No Tables provided!");
			} catch (NullPointerException e) {
				e.printStackTrace();
			}
		}
		log.info("Complete");
	}
}
