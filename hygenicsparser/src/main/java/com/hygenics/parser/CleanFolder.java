package com.hygenics.parser;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;

import javax.annotation.Resource;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import com.hygenics.scala.FileOps;

/**
 * This class cleans out files based on whether the record actually exists in a
 * database. Simple, clean, easy. This class leaves directory folders untouched
 * so the structure remains the same.
 * 
 * Just specify the directory path,avoidance String,recursiveness,query column
 * 
 * @param dirPath
 *            --String (path to base directory)
 * @param avoidanceString
 *            --String (files to avoid)
 * @param recursive
 *            -- boolean (whether or not to avoid new directories -- defualt is
 *            true)
 * @param column
 *            -- String (column from query to use)
 * @param replace
 *            --String replacement pattern for file to be used in checking
 *            against the string
 * @param emptyOnly
 *            --Boolean to get rid of empty files only
 * 
 * @author asevans
 *
 */
public class CleanFolder {
	private Logger log = LoggerFactory.getLogger(MainApp.class);

	// auto
	private getDAOTemplate template;

	// setable
	private boolean emptyOnly = false;
	private String basedirectory;
	private String avoidanceString;
	private boolean recursive = true;
	private String column;
	private String replacementPattern;
	private String table;

	public CleanFolder() {

	}// empty constructor

	/**
	 * Getters and Setters
	 * 
	 */

	public String getAvoidanceString() {
		return avoidanceString;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public getDAOTemplate getTemplate() {
		return template;
	}

	@Required
	@Resource(name = "getDAOTemplate")
	public void setTemplate(getDAOTemplate template) {
		this.template = template;
	}

	public boolean isEmptyOnly() {
		return emptyOnly;
	}

	public void setEmptyOnly(boolean emptyOnly) {
		this.emptyOnly = emptyOnly;
	}

	public String getReplacementPattern() {
		return replacementPattern;
	}

	public void setReplacementPattern(String replacementPattern) {
		this.replacementPattern = replacementPattern;
	}

	public String getBasedirectory() {
		return basedirectory;
	}

	public void setBasedirectory(String basedirectory) {
		this.basedirectory = basedirectory;
	}

	public void setAvoidanceString(String avoidanceString) {
		this.avoidanceString = avoidanceString;
	}

	public boolean isRecursive() {
		return recursive;
	}

	public void setRecursive(boolean recursive) {
		this.recursive = recursive;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public void run() {
		log.info("Starting Clean Up @ "
				+ Calendar.getInstance().getTime().toString());
		// get and make list
		log.info("SELECT " + column + " FROM " + table);
		ArrayList<String> arr = template.getArrayList("SELECT " + column + " FROM " + table, column);
		FileOps fops = new FileOps();
		fops.cleanDirectory(new java.io.File(basedirectory), arr, avoidanceString, emptyOnly, replacementPattern, recursive);
		
		// delete
		log.info("Cleaning");
		log.info("Initial Size: " + new File(basedirectory).list().length);
	
		log.info("Final Size: " + new File(basedirectory).list().length);
		log.info("Clean Up Complete @ "+ Calendar.getInstance().getTime().toString());
	}// run
}
