package com.hygenics.parser;

import java.util.Calendar;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hygenics.jdbc.jdbcconn;

/**
 * Dumps a File From command Line using specified delimeter, database, username,
 * and password in pgsql. Takes in a List<String> of tables to dump. If not
 * using pgsql, contact me and use dump to text for small files. It has
 * reasonable speed and will not crash for files under 80,000 rows.
 * 
 * @author aevans
 *
 */
public class CommandDump {
	private final static String datestamp= Calendar.getInstance().getTime().toString().trim().replaceAll(":|\\s", "");
	
	private final Logger log = LoggerFactory.getLogger(MainApp.class);
	private String delimiter = "|";
	private getDAOTemplate template;
	private Map<String, String> selects;
	private String url;
	private String user;
	private String pass;
	private long termtime = 60000;

	public CommandDump() {

	}

	public long getTermtime() {
		return termtime;
	}

	public void setTermtime(long termtime) {
		this.termtime = termtime;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPass() {
		return pass;
	}

	public void setPass(String pass) {
		this.pass = pass;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public getDAOTemplate getTemplate() {
		return template;
	}

	public void setTemplate(getDAOTemplate template) {
		this.template = template;
	}

	public Map<String, String> getSelects() {
		return selects;
	}

	public void setSelects(Map<String, String> selects) {
		this.selects = selects;
	}

	protected class ToFile extends RecursiveAction {
		private final String sql;
		private final String select;

		public ToFile(final String sql, final String select) {
			this.sql = sql;
			this.select = select;
		}

		@Override
		public void compute() {
			jdbcconn conn = new jdbcconn(url, user, pass);
			conn.CopyOut(sql.trim(), (select.trim()+ datestamp + ".txt").trim());
		}
	}

	public void run() {

		ForkJoinPool fjp = new ForkJoinPool(Runtime.getRuntime()
				.availableProcessors());

		Set<String> keys = selects.keySet();

		for (String k : keys) {
			log.info("Dumping for " + selects.get(k));
			String sql = "COPY  (" + selects.get(k) + ") TO STDOUT DELIMITER '"
					+ delimiter.trim() + "'  CSV HEADER";
			fjp.execute(new ToFile(sql, k));
		}

		try {
			fjp.awaitTermination(60000, TimeUnit.MILLISECONDS);
			fjp.shutdown();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
