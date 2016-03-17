package com.hygenics.parser;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.poi.ss.formula.functions.T;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;

import com.eclipsesource.json.JsonObject;

/**
 * Generates reports and sends them using Jasper for readability. when certain
 * statistics differ or when the average record count is different from a
 * certain threshold percentage. Default constructor.
 * 
 * The goal here is to avoid unnecessary information in a small organization. If
 * a report is generated for every instance it will inundate everyone and useful
 * information will be lost. (250 processes as of 10/14, god knows how many
 * later which would be multiplied by the average rate of completion for a
 * timeframe)
 * 
 * The program is intended to generate reports based on statistics stored in a
 * general database. The project reports errors discovered over time from
 * statistics stored in a spcified schema. For instance, I have a crawler
 * schema, parsed records schema, and a statistics schema (stats) in both the
 * dev and production databases. The users config file or program needs to
 * sepcify: 1. jdbcTemplate -- getDAOTemplate (sorry for the naming) 2. Crawler
 * Table --crawlTable 3. Statistics Table -- statsTable 4. Crawl Table columns
 * --crawlcols Map<String,String> 5. Statistics table -- statscols
 * Map<String,String>
 * 
 * The program does not generate multiple reports.
 * 
 * Jasper is used to generate documents from specific tables.
 * 
 * @author aevans
 */
public class Report {
	private String fpath;
	private getDAOTemplate template;
	private String body;

	private String crawlTable;
	private String statsTable;
	private Map<String, String> tests;

	private double dropthreshold;
	private double alpha;
	private double tvalue;
	private String lastRunStats;

	private boolean sendAlways;

	private ArrayList<String> lastData;
	private ArrayList<String> statsData;
	private String lastJson;

	public double getTvalue() {
		return tvalue;
	}

	public void setTvalue(double tvalue) {
		this.tvalue = tvalue;
	}

	public ArrayList<String> getLastData() {
		return lastData;
	}

	public void setStatsData(ArrayList<String> statsData) {
		this.statsData = statsData;
	}

	public Map<String, String> getTests() {
		return tests;
	}

	public void setTests(Map<String, String> tests) {
		this.tests = tests;
	}

	public String getLastRunStats() {
		return lastRunStats;
	}

	public void setLastRunStats(String lastRunStats) {
		this.lastRunStats = lastRunStats;
	}

	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}

	public boolean isSendAlways() {
		return sendAlways;
	}

	public void setSendAlways(boolean sendAlways) {
		this.sendAlways = sendAlways;
	}

	public double getDropthreshold() {
		return dropthreshold;
	}

	public void setDropthreshold(double dropthreshold) {
		this.dropthreshold = dropthreshold;
	}

	public double getAlpha() {
		return alpha;
	}

	public void setAlpha(double alpha) {
		this.alpha = alpha;
	}

	public String getFpath() {
		return fpath;
	}

	public void setFpath(String fpath) {
		this.fpath = fpath;
	}

	public getDAOTemplate getTemplate() {
		return template;
	}

	@Required
	@Autowired
	@Resource(name = "getDAOTemplate")
	public void setTemplate(getDAOTemplate template) {
		this.template = template;
	}

	public String getCrawlTable() {
		return crawlTable;
	}

	public void setCrawlTable(String crawlTable) {
		this.crawlTable = crawlTable;
	}

	public String getStatsTable() {
		return statsTable;
	}

	public void setStatsTable(String statsTable) {
		this.statsTable = statsTable;
	}

	// get stats from database
	private ArrayList<String> getStats(String sql) {
		return this.template.getJsonData(sql);
	}// getStats

	private void post(String sql) {
		this.template.execute(sql);
	}

	// calculate the standard deviation
	private double standardDeviation(ArrayList<String> data, String column) {
		double deviation = 0;
		double avg = 0;
		double sumx = 0;
		for (String d : data) {
			new JsonObject();
			double x = Double.parseDouble(JsonObject.readFrom(d).get(column)
					.asString().replaceAll("[^0-9]", "").trim());
			avg += Math.pow(x, 2);
			sumx += x;
		}

		return Math.sqrt((avg - (Math.pow(sumx, 2)) / data.size())
				/ (data.size() - 1));
	}// standardDeviation

	private double calcAverage(String column, ArrayList<String> data) {
		double average = 0;
		for (String row : data) {
			new JsonObject();
			average += Double.parseDouble(JsonObject.readFrom(row).get(column)
					.asString().replaceAll("[^0-9]", "").trim());
		}
		return (average / data.size());

	}// calcAverage

	// hypothesis testing to see if the values differ too greatly. useful to see
	// if lengths and numbers are different
	// this is more useful if there is a grouping of data (CLT so numpoints>32
	// in lastRun)
	private boolean testGroups(String column, String matchcolumn) {
		// collect necessary statistics
		double sd1 = standardDeviation(statsData, column);
		double sd2 = standardDeviation(lastData, matchcolumn);
		double avg1 = calcAverage(column, statsData);
		double avg2 = calcAverage(matchcolumn, lastData);

		// perform test
		double sp = (statsData.size() - 1) * Math.pow(sd1, 2)
				+ (lastData.size() - 1) * Math.pow(sd2, 2);
		sp /= (statsData.size() + lastData.size() - 2);
		sp = Math.sqrt(sp);

		double t = (avg1 - avg2) / sp * Math.sqrt(((1 / statsData.size()) + (1 / lastData.size())));
		

		return false;
	}// testGroups

	// calculate the outliers, this is useful if there are not groupings of data
	// (CLT so numpoints<32 in lastRun)
	private int outlier(String column, String matchcolumn) {
		// find outliers
		double sd = standardDeviation(statsData, column);
		double average = calcAverage(column, statsData);
		double lastaverage = calcAverage(matchcolumn, lastData);

		if (((average + sd) - lastaverage) > 1.5 * sd
				|| ((average - sd) - lastaverage) < 1.5 * sd) {
			// is an outlier
			return 1;
		} else if (((average + sd) - lastaverage) > 1.5 * sd
				|| ((average - sd) - lastaverage) < 1.5 * sd) {
			// is an extreme outlier
			return 2;
		}

		return 0;

	}// outlier

	// used to attach the statistics from the previous run
	private void addAttachment() {

	}// addAttachment

	// a toString like method for creating the email body
	private void addToBody(String statement) {
		body += statement + "\n";
	}// generateBody

	// sends the email only when requested
	private void sendMail() {

	}// sendMail

	/**
	 * Run the Reporting tool. Will Add an attachment showing statistics as
	 * well.
	 */
	public void run() {
		boolean sendReport = false;
		int count = 0;
		double averageTime = 0;
		double averageproxies = 0;

		String sql = "INSERT INTO " + statsTable;
		body = (body == null) ? "REPORT "
				+ Calendar.getInstance().getTime().toString() + "\n"
				: "REPORT " + Calendar.getInstance().getTime().toString()
						+ "\n" + body;

		if (sendAlways == true) {
			addToBody("SET TO ALWAYS SEND REPORT");
			sendMail();

		} else {
			// get stats

			// condense stats

			// check important columns

			// send report if something is deemed different
			if (sendReport == true) {

			}

			// add current run statistics to stats table
		}
	}
}
