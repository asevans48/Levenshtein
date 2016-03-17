package com.hygenics.parser;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

import javax.annotation.Resource;

import org.apache.commons.lang3.StringEscapeUtils;
import org.jfree.util.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;

import com.eclipsesource.json.JsonObject;
import com.hygenics.exceptions.MismatchException;
import com.hygenics.exceptions.NoDataException;

/**
 * JDump is set up to dump files from a database to a file when a standard copy
 * out fails. It grabs all records with headers as they are using the faster FJP
 * and dumps them to the file.
 * 
 * It is configured to work with Spring and grabs all records with proven
 * reliability. The activity stack value is any lower/upper case variant of
 * JDump which calls (JDump\d+ (\d+ is the sequence number)).
 * 
 * It does not allow for construction of the query output. Please Use Command
 * Dump, Pentaho, or SpecDump for this. SpecDump will Likely be Updated to
 * Accommodate this class when there is more time. At that point it will be
 * obsolete and maintained for old code.
 * 
 * @author asevans
 *
 */
public class JDump {
	private Logger log = LoggerFactory.getLogger(MainApp.class);
	private int qnum = Runtime.getRuntime().availableProcessors();
	private getDAOTemplate template;
	private Map<String, Map<String, Integer>> fpaths;// map with
														// Map<String,Map<String,Int>>
														// in format
														// Map<table,Map<fpath,Offset>>
	private Map<String, List<String>> headers;// map whose key should be the
												// table from the fpaths
												// containing a list of headers
												// in the order to be printed
	private boolean append = false;// overwrites by default
	private String delimeter = ",";
	private String replacementPattern;
	private String pullid;
	private String extracondition;
	private int pullsize;
	private int procnum;
	private boolean addFileDate = true;
	private String delimreplace = ";";
	private boolean archive = true;

	public JDump() {

	}

	public boolean isArchive() {
		return archive;
	}

	public void setArchive(boolean archive) {
		this.archive = archive;
	}

	public boolean isAddFileDate() {
		return addFileDate;
	}

	public void setAddFileDate(boolean addFileDate) {
		this.addFileDate = addFileDate;
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

	public Map<String, Map<String, Integer>> getFpaths() {
		return fpaths;
	}

	public void setFpaths(Map<String, Map<String, Integer>> fpaths) {
		this.fpaths = fpaths;
	}

	public Logger getLog() {
		return log;
	}

	public void setLog(Logger log) {
		this.log = log;
	}

	public int getQnum() {
		return qnum;
	}

	public void setQnum(int qnum) {
		this.qnum = qnum;
	}

	public String getPullid() {
		return pullid;
	}

	public void setPullid(String pullid) {
		this.pullid = pullid;
	}

	public String getExtracondition() {
		return extracondition;
	}

	public void setExtracondition(String extracondition) {
		this.extracondition = extracondition;
	}

	public int getPullsize() {
		return pullsize;
	}

	public void setPullsize(int pullsize) {
		this.pullsize = pullsize;
	}

	public int getProcnum() {
		return procnum;
	}

	public void setProcnum(int procnum) {
		this.procnum = procnum;
	}

	public boolean isAppend() {
		return append;
	}

	public void setAppend(boolean append) {
		this.append = append;
	}

	public String getDelimeter() {
		return delimeter;
	}

	public void setDelimeter(String delimeter) {
		this.delimeter = delimeter;
	}

	public String getReplacementPattern() {
		return replacementPattern;
	}

	public void setReplacementPattern(String replacementPattern) {
		this.replacementPattern = replacementPattern;
	}

	public Map<String, List<String>> getHeaders() {
		return headers;
	}

	public void setHeaders(Map<String, List<String>> headers) {
		this.headers = headers;
	}

	// private callable for getting data
	private class SplitQuery implements Callable<ArrayList<String>> {

		private static final long serialVersionUID = -8355795798235011672L;
		private final getDAOTemplate template;
		private String sql;

		private SplitQuery(final getDAOTemplate template, final String sql) {
			this.template = template;
			this.sql = sql;
		}

		@Override
		public ArrayList<String> call() {
			log.info("GETTING " + sql);
			ArrayList<String> results = template.getJsonData(sql);
			return results;
		}
	}

	// performs the dump
	private void toFile() {
		ArrayList<String> archs = new ArrayList<String>();
		List<Future<ArrayList<String>>> qfutures;
		Set<Callable<ArrayList<String>>> qcollect = new HashSet<Callable<ArrayList<String>>>(
				4);

		ForkJoinPool fjp = new ForkJoinPool((int) Math.ceil(Runtime
				.getRuntime().availableProcessors() * procnum));

		int dumped = 0;

		if (archive) {
			log.info("Cleaning");
			for (String k : fpaths.keySet()) {
				String fpath = "";

				for (String ofp : fpaths.get(k).keySet()) {
					fpath = ofp;
				}

				if (fpath.length() > 0) {
					String[] barr = fpath.split("\\/");
					String basefile = "";
					Archiver zip = new Archiver();
					for (int i = 0; i > barr.length - 1; i++) {
						basefile += (i == 0) ? barr[i] : "/" + barr[i];
					}
					if (basefile.trim().length() > 0) {
						zip.setBasedirectory(basefile);
						zip.setZipDirectory(basefile + "archive.zip");
						zip.setAvoidanceString(".zip|archive");
						zip.setDelFiles(true);
						zip.run();
					}
				}
			}
		}

		log.info("Dumping");
		for (String table : fpaths.keySet()) {
			int offset = 0;
			if (template.checkTable(table, table.split("\\.")[0])) {
				if (template.getCount(table) > 0) {
					log.info("Dumping for " + table);
					// get header
					String select = "SELECT * FROM " + table;
					String fpath = null;
					ArrayList<String> jsons;
					String condition;
					int w = 0;
					int start = offset;
					int chunksize = (int) Math.ceil(pullsize / qnum);

					// get fpath
					for (String ofp : fpaths.get(table).keySet()) {
						start = fpaths.get(table).get(ofp);
						fpath = ofp;
					}

					// perform write
					if (headers != null && fpath != null) {
						List<String> headersList = headers.get(table);

						String output = null;
						boolean existed = true;

						if (addFileDate) {
							fpath = fpath
									+ Calendar.getInstance().getTime()
											.toString().trim()
											.replaceAll(":|\\s", "") + ".txt";
						}

						// check to see if file should be created
						if (!new File(fpath).exists()) {

							try {
								new File(fpath).createNewFile();
							} catch (IOException e) {
								e.printStackTrace();
							}
							existed = false;
						}

						// check to see if file must be recreated
						if (!append) {

							File f = new File(fpath);
							f.delete();
							try {
								f.createNewFile();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}

						if (headersList != null
								&& (append == false || existed == false)) {
							for (String header : headersList) {
								output = (output == null) ? StringEscapeUtils
										.unescapeXml(header) : output
										+ delimeter
										+ StringEscapeUtils.unescapeXml(header);
							}
						}

						do {

							// get records
							jsons = new ArrayList<String>(pullsize);
							log.info("Looking for Pages.");
							for (int conn = 0; conn < qnum; conn++) {
								// create condition
								condition = " WHERE "
										+ pullid
										+ " >= "
										+ (start + (conn * chunksize))
										+ " AND "
										+ pullid
										+ " < "
										+ Integer.toString(start
												+ (chunksize * (conn + 1)));

								if (extracondition != null) {
									condition += " " + extracondition.trim();
								}

								// get queries
								qcollect.add(new SplitQuery(template,
										(select + condition)));
								log.info("Fetching " + select + condition);
							}

							start += (chunksize * qnum);

							qfutures = fjp.invokeAll(qcollect);

							w = 0;
							while (fjp.getActiveThreadCount() > 0
									&& fjp.isQuiescent() == false) {
								w++;
							}
							log.info("Waited for " + w + " cycles");

							for (Future<ArrayList<String>> f : qfutures) {
								try {

									ArrayList<String> test = f.get();
									if (test != null) {
										if (test.size() > 0) {
											jsons.addAll(test);
										}
									}

									if (f.isDone() == false) {
										f.cancel(true);
									}

									f = null;
								} catch (Exception e) {
									log.warn("Encoding Error!");
									e.printStackTrace();
								}
							}
							qcollect = new HashSet<Callable<ArrayList<String>>>(
									4);
							qfutures = null;
							log.info("Finished Getting Pages");

							// post records to the file
							try (FileWriter fw = new FileWriter(
									new File(fpath), true)) {
								// get and write headers

								if (jsons.size() > 0) {
									fw.write(output + "\n");
									// write data
									for (String json : jsons) {
										output = null;
										JsonObject jo = JsonObject
												.readFrom(json);
										if (jo.size() >= headersList.size()) {// allows
																				// trimming
																				// of
																				// table
																				// to
																				// key
																				// aspects
											output = null;

											for (String key : headers
													.get(table)) {

												if (jo.get(key.toLowerCase()) != null) {
													String data = StringEscapeUtils
															.unescapeXml(jo
																	.get(key.toLowerCase())
																	.asString());

													if (replacementPattern != null) {
														data = data
																.replaceAll(
																		replacementPattern,
																		"");
														data = data.replace(
																delimeter,
																delimreplace);
													}

													output = (output == null) ? data
															.replaceAll(
																	"[^\u0020-\u0070 ]+",
																	"")
															: output
																	+ delimeter
																	+ data.replaceAll(
																			"[^\u0020-\u0070 ]+",
																			"");
												} else {
													output += delimeter;
												}
											}

											if (output != null
													&& output.trim().length() > headersList
															.size()) {
												fw.write(output + "\n");
											}
										} else {
											if (jsons.size() == 0) {
												Log.info("Number of Headers and Keys from Json Array and Headers List Impossible to Match");
												try {
													throw new MismatchException(
															"Number of Headers: "
																	+ headersList
																			.size()
																	+ " && Number of Keys: "
																	+ jo.size());
												} catch (MismatchException e) {
													e.printStackTrace();
												}
											}
										}

										output = null;
									}
								} else {
									log.info("EOF FOUND! No New Records in This Iteration....Stopping.");
								}
							} catch (IOException e) {
								e.printStackTrace();
							}

						} while (jsons.size() > 0);

					} else {
						try {
							throw new NullPointerException(
									"No Headers Input to Class. Please Create the Requisite Map.");
						} catch (NullPointerException e) {
							e.printStackTrace();
						}
					}
					dumped += 1;
				} else {
					try {
						throw new NoDataException("No Data Found in Table "
								+ table);
					} catch (NoDataException e) {
						e.printStackTrace();
					}
				}
			} else {
				log.info("Missing Table " + table);
				try {
					throw new NullPointerException("Table " + table
							+ " Does Not Exist!!!");
				} catch (NullPointerException e) {
					e.printStackTrace();
				}
			}
		}// end LOOP

		if (!fjp.isShutdown()) {
			fjp.shutdownNow();
		}

		if (dumped == 0) {
			log.error("No Data Found in Any Table");
			System.exit(-1);
		}
	}// toFile

	public void run() {
		log.info("Starting to Dump @ "
				+ Calendar.getInstance().getTime().toString());
		toFile();
		log.info("Finished Dump @ "
				+ Calendar.getInstance().getTime().toString());
	}// public run helper method
}