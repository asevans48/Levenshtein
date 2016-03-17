package com.hygenics.parser;

import java.util.List;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;

/**
 * The Truncate Class Which Truncates a Table
 * 
 * @author aevans
 *
 */
public class Truncate {

	private Logger log = LoggerFactory.getLogger(MainApp.class);
	private List<String> sql;
	private List<String> tables;
	private boolean cascade = true;
	private getDAOTemplate template;

	public Truncate() {

	}

	public getDAOTemplate getTemplate() {
		return template;
	}

	@Autowired
	@Required
	@Resource(name = "getDAOTemplate")
	public void setTemplate(getDAOTemplate template) {
		this.template = template;
	}

	public List<String> getSql() {
		return sql;
	}

	public void setSql(List<String> sql) {
		this.sql = sql;
	}

	@Required
	public boolean isCascade() {
		return cascade;
	}

	public void setCascade(boolean cascade) {
		this.cascade = cascade;
	}

	public List<String> getTables() {
		return tables;
	}

	public void setTables(List<String> tables) {
		this.tables = tables;
	}

	public void truncate() {
		String update = null;

		if (tables != null) {
			for (String table : tables) {
				log.info("Cascading " + table);
				update = (cascade) ? "TRUNCATE " + table + "CASCADE"
						: "TRUNCATE " + table;
				this.template.execute(update);
			}
		}

		if (sql != null) {
			for (String q : sql) {
				this.template.execute(q);
			}
		}
	}

}
