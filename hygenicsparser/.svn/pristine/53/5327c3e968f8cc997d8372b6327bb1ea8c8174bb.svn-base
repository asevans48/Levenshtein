package com.hygenics.parser;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.CookieHandler;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RecursiveAction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Resource;
import javax.imageio.ImageIO;
import javax.net.ssl.HttpsURLConnection;

import mjson.Json;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;

import com.eclipsesource.json.JsonObject;
import com.hygenics.imaging.ImageCleanup;

/**
 * Hopefully, this is only needed to populate images from a file but dynamically
 * created links seem to pose a challenge.
 * 
 * This class pulls images from a mapping and puts them into a table using the
 * basic jdbcconn (there is only one query needed).If an image requires a
 * cookie, then allow for the images to be grabbed with the individual pages
 * using the appropriate command.
 * 
 * An offenderhash and image link are required. The offenderhash is used in the
 * metadata. You can specify that the offenderhash be a foreign key constraint
 * which may help in data deduplication. Otherwise, stay away from constraints.
 * 
 * Constructor is default becaus class is already default.
 * 
 * @author aevans
 *
 */
class GetImages extends ImageCleanup {
	private Logger log = LoggerFactory.getLogger(MainApp.class.getName());

	private boolean removeRed;
	private int factor = 2;
	private boolean skipGrab = false;
	private boolean overwrite = false;
	private int numqueries = 4;
	private int commitsize = 100;
	private int offset = 0;
	private String conditions;

	private int iterations = 1;
	private boolean tillfound = false;

	private String extracondition;
	private String dbcondition;
	private String table;
	private String imageColumn;
	private String urlColumn;
	private String baseurlcolumn;
	private String prefixcolumn;
	private String postfixcolumn;
	private String hashcolumn;
	private String idString;
	private String baseurl;

	private String proxyuser;
	private String proxypass;
	private String proxyhost;
	private String proxyport;
	private int maxproxies = 0;
	private String proxies;

	private List<String> imagecommands;
	private List<String> extracols;
	private List<String> metacols;

	private String sql;
	private String cookieurl;
	private String imagepostfix = ".jpg";

	private Map<String, String> cookieparams;
	private Map<String, String> postparams;
	private Map<String, String> headers;

	private boolean addtoDB = true;
	private boolean addFromFile = false;

	private String fpath;
	private boolean cleanup;

	private boolean https;
	private getDAOTemplate template;
	private long timeout = (int) Math.round((Math.random() * 10000));

	public boolean isRemoveRed() {
		return removeRed;
	}

	public void setRemoveRed(boolean removeRed) {
		this.removeRed = removeRed;
	}

	public int getFactor() {
		return factor;
	}

	public void setFactor(int factor) {
		this.factor = factor;
	}

	public String getExtracondition() {
		return extracondition;
	}

	public void setExtracondition(String extracondition) {
		this.extracondition = extracondition;
	}

	public String getDbcondition() {
		return dbcondition;
	}

	public void setDbcondition(String dbcondition) {
		this.dbcondition = dbcondition;
	}

	public int getMaxproxies() {
		return maxproxies;
	}

	public void setMaxproxies(int maxproxies) {
		this.maxproxies = maxproxies;
	}

	public boolean isSkipGrab() {
		return skipGrab;
	}

	public void setSkipGrab(boolean skipGrab) {
		this.skipGrab = skipGrab;
	}

	public boolean isOverwrite() {
		return overwrite;
	}

	public void setOverwrite(boolean overwrite) {
		this.overwrite = overwrite;
	}

	public boolean isTillfound() {
		return tillfound;
	}

	public void setTillfound(boolean tillfound) {
		this.tillfound = tillfound;
	}

	/**
	 * Get Whether to cleanup the specified directory, removing all files
	 * 
	 * @return -boolean
	 */
	public boolean isCleanup() {
		return cleanup;
	}

	public String getProxies() {
		return proxies;
	}

	public void setProxies(String proxies) {
		this.proxies = proxies;
	}

	/**
	 * Specify whether to cleanup the fpath directory, removes all files
	 * 
	 * @param cleanup
	 *            -boolean
	 */
	public void setCleanup(boolean cleanup) {
		this.cleanup = cleanup;
	}

	/**
	 * The number of iterations for each term
	 * 
	 * @return int
	 */
	public int getIterations() {
		return iterations;
	}

	/**
	 * Set the number of iterations per query. Auto is 1.
	 * 
	 * @param iterations
	 *            -int
	 */
	public void setIterations(int iterations) {
		this.iterations = iterations;
	}

	/**
	 * Whether to add to the DB. Either this should be set or addFromFile but
	 * not both to avoid duplicates.
	 * 
	 * @return boolean
	 */
	public boolean isAddtoDB() {
		return addtoDB;
	}

	/**
	 * Set whether to add directly to the database. Turn off if adding from
	 * file. Default is true.
	 * 
	 * @param addtoDB
	 *            -boolean
	 */
	public void setAddtoDB(boolean addtoDB) {
		this.addtoDB = addtoDB;
	}

	/**
	 * Check whether to add from a file. Should be off if adding directly from
	 * the database.
	 * 
	 * @return boolean
	 */
	public boolean isAddFromFile() {
		return addFromFile;
	}

	public void setAddFromFile(boolean addFromFile) {
		this.addFromFile = addFromFile;
	}

	public List<String> getExtracols() {
		return extracols;
	}

	public void setExtracols(List<String> extracols) {
		this.extracols = extracols;
	}

	public String getBaseurl() {
		return baseurl;
	}

	public int getNumqueries() {
		return numqueries;
	}

	public void setNumqueries(int numqueries) {
		this.numqueries = numqueries;
	}

	public String getConditions() {
		return conditions;
	}

	public void setConditions(String conditions) {
		this.conditions = conditions;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public int getCommitsize() {
		return commitsize;
	}

	public void setCommitsize(int commitsize) {
		this.commitsize = commitsize;
	}

	public List<String> getMetacols() {
		return metacols;
	}

	public void setMetacols(List<String> metacols) {
		this.metacols = metacols;
	}

	public String getIdString() {
		return idString;
	}

	public void setIdString(String idString) {
		this.idString = idString;
	}

	public List<String> getImagecommands() {
		return imagecommands;
	}

	// cleans up the metadata
	public void setImagecommands(List<String> imagecommands) {
		this.imagecommands = imagecommands;
	}

	public String getImagepostfix() {
		return imagepostfix;
	}

	public void setImagepostfix(String imagepostfix) {
		this.imagepostfix = imagepostfix;
	}

	public String getPrefixcolumn() {
		return prefixcolumn;
	}

	public void setPrefixcolumn(String prefixcolumn) {
		this.prefixcolumn = prefixcolumn;
	}

	public String getHashcolumn() {
		return hashcolumn;
	}

	@Required
	public void setHashcolumn(String hashcolumn) {
		this.hashcolumn = hashcolumn;
	}

	public String getPostfixcolumn() {
		return postfixcolumn;
	}

	public void setPostfixcolumn(String postfixcolumn) {
		this.postfixcolumn = postfixcolumn;
	}

	public String getFpath() {
		return fpath;
	}

	@Required
	public void setFpath(String fpath) {
		this.fpath = fpath;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public String getImageColumn() {
		return imageColumn;
	}

	public void setImageColumn(String imageColumn) {
		this.imageColumn = imageColumn;
	}

	public String getUrlColumn() {
		return urlColumn;
	}

	public void setUrlColumn(String urlColumn) {
		this.urlColumn = urlColumn;
	}

	public String getBaseurlcolumn() {
		return baseurlcolumn;
	}

	public void setBaseurlcolumn(String baseurlcolumn) {
		this.baseurlcolumn = baseurlcolumn;
	}

	public Map<String, String> getHeaders() {
		return headers;
	}

	public void setHeaders(Map<String, String> headers) {
		this.headers = headers;
	}

	public String getProxyuser() {
		return proxyuser;
	}

	public void setProxyuser(String proxyuser) {
		this.proxyuser = proxyuser;
	}

	public String getProxypass() {
		return proxypass;
	}

	public void setProxypass(String proxypass) {
		this.proxypass = proxypass;
	}

	public String getProxyhost() {
		return proxyhost;
	}

	public void setProxyhost(String proxyhost) {
		this.proxyhost = proxyhost;
	}

	public String getProxyport() {
		return proxyport;
	}

	public void setProxyport(String proxyport) {
		this.proxyport = proxyport;
	}

	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	public long getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = (int) Math.round((Math.random() * timeout));
	}

	public void setBaseurl(String baseurl) {
		this.baseurl = baseurl;
	}

	public String getCookieurl() {
		return cookieurl;
	}

	public void setCookieurl(String cookieurl) {
		this.cookieurl = cookieurl;
	}

	public Map<String, String> getCookieparams() {
		return cookieparams;
	}

	public void setCookieparams(Map<String, String> cookieparams) {
		this.cookieparams = cookieparams;
	}

	public Map<String, String> getPostparams() {
		return postparams;
	}

	public void setPostparams(Map<String, String> postparams) {
		this.postparams = postparams;
	}

	public boolean isHttps() {
		return https;
	}

	public void setHttps(boolean https) {
		this.https = https;
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

	private class ImagePost extends RecursiveAction {
		private final List<String> json;

		ImagePost(List<String> json) {
			this.json = json;
		}

		@Override
		protected void compute() {
			ArrayList<String> jarr = new ArrayList<String>();
			jarr.addAll(this.json);
			if (json.size() > 0) {

				Map<String, Json> jmap = Json.read(json.get(0)).asJsonMap();
				String sql = "INSERT INTO " + jmap.get("table").asString()
						+ " (";
				String vals = "(";
				Set<String> keys = jmap.keySet();
				Iterator<String> it = keys.iterator();
				for (int i = 0; i < keys.size(); i++) {
					String nt = it.next();

					if (nt.toLowerCase().compareTo("table") != 0) {
						sql += nt;
						vals += "?";
						if ((i + 1) < keys.size() - 1) {
							sql += ",";
							vals += ",";
						}
					}
				}
				sql += ") VALUES" + vals + ")";
				log.info(sql);
				template.postJsonData(sql, jarr);
			}
		}

	}// ImagePost

	private class QueryDatabase implements Callable<ArrayList<String>> {
		private final String sql;

		QueryDatabase(String sql) {
			this.sql = sql;
		}

		@Override
		public ArrayList<String> call() throws Exception {
			log.info(sql);
			return template.getJsonData(sql);
		}
	}// QueryDatabase

	/**
	 * Private Inner Class that Can Grab Multiple Images at Once
	 * 
	 * @author aevans
	 *
	 */
	private class ImageGrabber implements Callable<String> {
		private final String ipath;
		private final String proxy;

		public ImageGrabber(final String ipath, final String proxy) {
			this.ipath = ipath;
			this.proxy = proxy;
		}

		@Override
		public String call() throws Exception {
			JsonObject jobj = new JsonObject();
			Map<String, Json> jimage = Json.read(this.ipath).asJsonMap();
			if (new File(fpath + jimage.get(hashcolumn).asString()
					+ imagepostfix).exists() == false
					|| overwrite == true) {

				if (overwrite == true
						&& new File(fpath + jimage.get(hashcolumn).asString()
								+ imagepostfix).exists() == true) {
					new File(fpath + jimage.get(hashcolumn).asString()
							+ imagepostfix).delete();
				}

				try {
					Thread.sleep(Math.round((Math.random() * timeout)));
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}

				try {
					if (imageColumn != null) {
						log.info("Getting "
								+ jimage.get(imageColumn).asString() + " FOR "
								+ jimage.get(hashcolumn).asString());
					} else if (baseurlcolumn != null) {
						log.info("Getting "
								+ jimage.get(baseurlcolumn).asString()
								+ " FOR " + jimage.get(hashcolumn).asString());
					}

					// get the cookie
					if (cookieurl != null) {
						log.info("Cookie Page Found");
						// get the cookie url
						URL url = new URL(cookieurl);
						String cookieparamstring = null;

						if (cookieparams != null) {
							cookieparamstring = formatCookies(cookieparams);
						}

						if (https) {
							HttpsURLConnection conns = (HttpsURLConnection) url
									.openConnection();

							for (String param : headers.keySet()) {
								conns.setRequestProperty(URLEncoder.encode(
										param, "UTF-8"), URLEncoder.encode(
										headers.get(param), "UTF-8"));
							}

							if (cookieparamstring != null) {
								conns.setDoOutput(true);
								conns.setRequestMethod("POST");
								conns.setRequestProperty("Content-Length",
										Integer.toString(cookieparamstring
												.length()));
								conns.getOutputStream().write(
										cookieparamstring.getBytes());
							}

							conns.getContent();
							byte[] ibytes = IOUtils.toByteArray(conns
									.getInputStream());

							conns.disconnect();

						} else {
							HttpURLConnection conn = (HttpURLConnection) url
									.openConnection();

							for (String param : headers.keySet()) {
								conn.setRequestProperty(URLEncoder.encode(
										param, "UTF-8"), URLEncoder.encode(
										headers.get(param), "UTF-8"));
							}

							if (cookieparamstring != null) {
								conn.setDoOutput(true);
								conn.setRequestMethod("POST");
								conn.setRequestProperty("Content-Length",
										Integer.toString(cookieparamstring
												.length()));
								conn.getOutputStream().write(
										cookieparamstring.getBytes());
							}
							conn.getContent();
							byte[] ibytes = IOUtils.toByteArray(conn
									.getInputStream());
							conn.disconnect();
						}

					}
					byte[] ibytes = null;
					if (baseurl != null || baseurlcolumn != null) {
						log.info("Getting Image");
						// PARAMSTRING SETTING
						String paramstring = null;
						if (postparams != null) {
							paramstring = formatCookies(postparams);
						}

						String iurl = "";

						if (baseurl != null) {
							iurl += baseurl;
						}

						if (baseurlcolumn != null) {
							iurl += jimage.get(baseurlcolumn).asString();
						}

						if (imageColumn != null) {
							iurl += jimage.get(imageColumn).asString();
						}

						log.info("Final URL: " + iurl);

						// get images
						if (https) {

							HttpsURLConnection conns = (HttpsURLConnection) new URL(
									iurl).openConnection();

							for (String param : headers.keySet()) {
								conns.setRequestProperty(URLEncoder.encode(
										param, "UTF-8"), URLEncoder.encode(
										headers.get(param), "UTF-8"));
							}

							if (paramstring != null) {
								conns.setDoOutput(true);
								conns.setRequestMethod("POST");
								conns.setRequestProperty("Content-Length",
										Integer.toString(paramstring.length()));
								conns.getOutputStream().write(
										paramstring.getBytes());
							} else {
								conns.setRequestMethod("GET");
							}

							conns.getContent();
							ibytes = IOUtils
									.toByteArray(conns.getInputStream());
							conns.disconnect();
						} else {
							HttpURLConnection conn = (HttpURLConnection) new URL(
									iurl).openConnection();

							for (String param : headers.keySet()) {
								conn.setRequestProperty(param,
										headers.get(param));
							}

							if (paramstring != null) {
								conn.setDoOutput(true);
								conn.setRequestMethod("POST");
								conn.setRequestProperty("Content-Length",
										Integer.toString(paramstring.length()));
								conn.getOutputStream().write(
										paramstring.getBytes());
							} else {
								conn.setRequestMethod("GET");
							}
							ibytes = IOUtils.toByteArray(conn.getInputStream());
							conn.getContent();
							conn.disconnect();
						}

						if (ibytes != null && ibytes.length > 0) {
							// write metadata
							if (metacols != null) {
								String data = null;
								for (String col : metacols) {
									data = (data == null) ? jimage.get(col)
											.asString() : data
											+ jimage.get(col).asString();
								}
								writeMetaData(data, ibytes);
							}

							// call image commands
							if (imagecommands != null) {
								log.info("Manipulating Image");
								for (String command : imagecommands) {

									if (command.toLowerCase().compareTo(
											"blackandwhite") == 0) {
										setBlackandWhite(ibytes);
									} else if (command.toLowerCase().contains(
											"reaverage")) {
										Pattern p = Pattern.compile("[0-9]+");
										Matcher m = p.matcher(command);

										if (m.find()) {
											ibytes = reaverage(
													Integer.parseInt(m.group(0)),
													ibytes);
										}
									} else if (command.toLowerCase().contains(
											"sharpen")) {
										Pattern p = Pattern.compile("[0-9]+");
										Matcher m = p.matcher(command);

										if (m.find()) {
											ibytes = sharpen(
													ibytes,
													Integer.parseInt(m.group(0)),
													imagepostfix);
										}
									}
								}
							}

							if (removeRed == true) {
								ImageCleanup ic = new ImageCleanup();
								ibytes = ic.reaverage(factor, ibytes);
							}

							// save image
							log.info("Saving to " + fpath
									+ jimage.get(hashcolumn).asString()
									+ imagepostfix);

							try {
								ByteArrayInputStream bis = new ByteArrayInputStream(
										ibytes);
								BufferedImage bi = ImageIO.read(bis);

								if (bi.getHeight() > 0 && bi.getWidth() > 0) {
									File fp = new File(fpath
											+ jimage.get(hashcolumn).asString()
											+ imagepostfix);
									if (!fp.exists()) {
										fp.createNewFile();
									}
									// write image
									ImageIO.write(bi, "jpg", fp);

									// add basics
									jobj.add("table", table)
											.add("image_path",
													jimage.get(hashcolumn)
															.asString()
															+ imagepostfix)
											.add("offenderhash",
													jimage.get(hashcolumn)
															.asString());

									// add date
									jobj.add("date", Calendar.getInstance()
											.getTime().toString());

									// extra data columns to be added
									if (extracols != null) {
										for (String column : extracols) {
											jobj.add(column, jimage.get(column)
													.toString());
										}
									}

								}
							} catch (Exception e) {
								e.printStackTrace();
							}
						} else {
							if (imageColumn != null) {
								log.info("No Image Found for "
										+ jimage.get(imageColumn).asString());
							} else if (baseurlcolumn != null) {
								log.info("No Image Found for "
										+ jimage.get(baseurlcolumn).asString());
							}
						}
					}
				} catch (MalformedURLException e) {
					log.info("Failed FOR " + jimage.get(hashcolumn).asString());
					e.printStackTrace();
				} catch (IOException e) {
					log.info("Failed FOR " + jimage.get(hashcolumn).asString());
					e.printStackTrace();
				}
				return jobj.toString();
			}

			return null;
		}
	}

	private void proxy(String host, String port, boolean https, String user,
			String pass) {
		if (https) {
			System.setProperty("https.proxyHost", host);
			System.setProperty("https.proxyPort", port);

			if (user != null) {
				System.setProperty("https.proxyUser", user);
			}

			if (pass != null) {
				System.setProperty("https.proxyPassword", pass);
			}
		} else {
			System.setProperty("http.proxyHost", host);
			System.setProperty("http.proxyPort", port);

			if (user != null) {
				System.setProperty("http.proxyUser", user);
			}

			if (pass != null) {
				System.setProperty("http.proxyPassword", pass);
			}
		}
	}// proxy

	private String encodeUrlParams(String[] innames, String[] invals) {
		int length = innames.length;
		String url_params = "";
		for (int i = 0; i < length; i++) {
			try {
				if (i == 0)
					url_params = URLEncoder.encode(innames[i], "UTF-8") + "="
							+ URLEncoder.encode(invals[i], "UTF-8");
				else
					url_params += "&" + URLEncoder.encode(innames[i], "UTF-8")
							+ "=" + URLEncoder.encode(invals[i], "UTF-8");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
		return url_params;
	}// encodeUrlParams

	private String formatCookies(Map<String, String> cookies) {
		// format cookies from map
		String cookiestring = null;
		if (cookies != null) {
			for (String key : cookies.keySet()) {
				try {
					cookiestring += URLEncoder.encode(key, "UTF-8") + "="
							+ URLEncoder.encode(cookies.get(key), "UTF-8")
							+ ";";
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return cookiestring;
	}// format cookies from map

	private void cleanupDir(String dirpath) {
		// delete all files in the directories

		// base case
		if (dirpath == null || new File(dirpath).list() == null) {
			return;
		}

		// delete paths
		for (String path : new File(dirpath).list()) {
			File f = new File(path);
			if (f.isDirectory()) {
				cleanupDir(path);
			} else {
				f.delete();
			}

		}

	}// cleanup

	private void getImages() {
		// controls the web process from a removed method
		log.info("Setting Up Pull");
		String[] proxyarr = (proxies == null) ? null : proxies.split(",");
		// cleanup
		if (cleanup) {
			cleanupDir(fpath);
		}

		// image grab
		CookieManager cm = new CookieManager();
		cm.setCookiePolicy(CookiePolicy.ACCEPT_ALL);
		CookieHandler.setDefault(cm);
		int numimages = 0;
		InputStream is;
		byte[] bytes;
		int iter = 0;
		int found = 0;

		// set proxy if needed
		if (proxyuser != null) {
			proxy(proxyhost, proxyport, https, proxyuser, proxypass);
		}

		int i = 0;
		ArrayList<String> postImages = new ArrayList<String>();
		ForkJoinPool fjp = new ForkJoinPool(Runtime.getRuntime()
				.availableProcessors());
		Set<Callable<String>> pulls = new HashSet<Callable<String>>();
		Set<Callable<ArrayList<String>>> sqls = new HashSet<Callable<ArrayList<String>>>();
		List<Future<String>> imageFutures;

		ArrayList<String> images;
		int chunksize = (int) Math.ceil(commitsize / numqueries);
		log.info("Chunksize: " + chunksize);
		if (baseurl != null || baseurlcolumn != null) {
			do {
				log.info("Offset: " + offset);
				log.info("Getting Images");
				images = new ArrayList<String>(commitsize);
				log.info("Getting Columns");
				for (int n = 0; n < numqueries; n++) {
					String tempsql = sql + " WHERE " + idString + " >= "
							+ offset + " AND " + idString + " < "
							+ (offset + chunksize);

					if (conditions != null) {
						tempsql += conditions;
					}

					sqls.add(new QueryDatabase(
							((extracondition != null) ? tempsql + " "
									+ extracondition : tempsql)));

					offset += chunksize;
				}

				List<Future<ArrayList<String>>> futures = fjp.invokeAll(sqls);

				int w = 0;
				while (fjp.isQuiescent() && fjp.getActiveThreadCount() > 0) {
					w++;
				}

				for (Future<ArrayList<String>> f : futures) {
					try {
						ArrayList<String> fjson;
						fjson = f.get();
						if (fjson.size() > 0) {
							images.addAll(fjson);
						}

						if (f.isDone() == false) {
							f.cancel(true);
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}
				}
				log.info(Integer.toString(images.size())
						+ " image links found. Pulling.");

				ArrayList<String> tempproxies = new ArrayList<String>();

				if (proxyarr != null) {
					for (String proxy : proxyarr) {
						tempproxies.add(proxy.trim());
					}
				}

				if (maxproxies > 0) {
					maxproxies -= 1;// 0 and 1 should be equivalent conditions
									// --num is not like most 0 based still due
									// to >=
				}

				// get images
				for (int num = 0; num < images.size(); num++) {
					String icols = images.get(num);
					int proxnum = (int) Math.random()
							* (tempproxies.size() - 1);
					String proxy = (tempproxies.size() == 0) ? null
							: tempproxies.get(proxnum);

					// add grab
					pulls.add(new ImageGrabber(icols, proxy));

					if (proxy != null) {
						tempproxies.remove(proxy);
					}

					// check for execution
					if (num + 1 == images.size() || pulls.size() >= commitsize
							|| tempproxies.size() == 0) {
						if (tempproxies.size() == 0 && proxies != null) {
							tempproxies = new ArrayList<String>(proxyarr.length);

							for (String p : proxyarr) {
								tempproxies.add(p.trim());
							}
						}

						imageFutures = fjp.invokeAll(pulls);
						w = 0;

						while (fjp.isQuiescent() == false
								&& fjp.getActiveThreadCount() > 0) {
							w++;
						}

						for (Future<String> f : imageFutures) {
							String add;
							try {
								add = f.get();

								if (add != null) {
									postImages.add(add);
								}
							} catch (InterruptedException e) {
								e.printStackTrace();
							} catch (ExecutionException e) {
								e.printStackTrace();
							}
						}
						imageFutures = null;// garbage collect elligible
						pulls = new HashSet<Callable<String>>(commitsize);
					}

					if (postImages.size() >= commitsize && addtoDB == true) {
						if (addtoDB) {
							log.info("Posting to Database");
							log.info("Found " + postImages.size() + " images");
							numimages += postImages.size();
							int size = (int) Math.floor(postImages.size()
									/ numqueries);
							for (int n = 0; n < numqueries; n++) {
								if (((n + 1) * size) < postImages.size()
										&& (n + 1) < numqueries) {
									fjp.execute(new ImagePost(postImages
											.subList(n * size, (n + 1) * size)));
								} else {
									fjp.execute(new ImagePost(postImages
											.subList(n * size,
													postImages.size() - 1)));
								}
							}

							w = 0;
							while (fjp.isQuiescent()
									&& fjp.getActiveThreadCount() > 0) {
								w++;
							}
						}
						found += postImages.size();
						postImages.clear();
					}

				}

				if (postImages.size() > 0 && addtoDB == true) {
					log.info("Posting to Database");
					numimages += postImages.size();
					int size = (int) Math.floor(postImages.size() / numqueries);
					for (int n = 0; n < numqueries; n++) {
						if (((n + 1) * size) < postImages.size()) {
							fjp.execute(new ImagePost(postImages.subList(n
									* size, (n + 1) * size)));
						} else {
							fjp.execute(new ImagePost(postImages.subList(n
									* size, postImages.size())));
						}
					}

					w = 0;
					while (fjp.isQuiescent() && fjp.getActiveThreadCount() > 0) {
						w++;
					}

					found += postImages.size();
					postImages.clear();
				}

				// handle iterations specs
				iter += 1;
				log.info("Iteration: " + iter);
				if ((iter < iterations && found < images.size())
						|| tillfound == true) {
					log.info("Not All Images Obtained Trying Iteration " + iter
							+ " of " + iterations);
					offset -= commitsize;
				} else if ((iter < iterations && found >= images.size())
						&& tillfound == false) {
					log.info("Images Obtained in " + iter
							+ " iterations. Continuing.");
					iter = 0;
				} else {
					// precautionary
					log.info("Images Obtained in " + iter
							+ " iterations. Continuing");
					iter = 0;
				}

			} while (images.size() > 0 && iter < iterations);

			if (fjp.isShutdown()) {
				fjp.shutdownNow();
			}
		}

		log.info("Complete. Check for Errors \n " + numimages + " Images Found");
	}// getImages

	// adds to the database from a file, must be specified and should not be run
	// if addtoDB is set to true
	private void addFromFile() {
		File f = new File(fpath);
		ForkJoinPool fjp = new ForkJoinPool(Runtime.getRuntime()
				.availableProcessors());
		ArrayList<String> imageData = new ArrayList<String>();
		int size = (int) Math.ceil(commitsize / numqueries);

		if (f.exists()) {
			// get the image data
			File[] list = f.listFiles();
			int curr = 0;
			if (list != null) {
				for (File img : list) {
					curr += 1;
					if (img.isDirectory() == false
							&& (img.getName().contains(".bmp")
									|| img.getName().toLowerCase()
											.contains(".jpg")
									|| img.getName().toLowerCase()
											.contains(".png") || img.getName()
									.toLowerCase().contains("jpeg"))) {
						try {
							if (dbcondition == null
									|| template
											.getJsonData(
													dbcondition
															.replace(
																	"$IMAGE$",
																	img.getName()
																			.replaceAll(
																					"(?mis)"
																							+ imagepostfix,
																					"")))
											.size() > 0) {
								BufferedImage bi = ImageIO.read(img);// only
																		// used
																		// to
																		// ensure
																		// that
																		// this
																		// is an
																		// image
								JsonObject jobj = new JsonObject();
								jobj.add(
										"offenderhash",
										img.getName().replaceAll(
												"(?mis)" + imagepostfix, ""));// assumes
																				// hash
																				// is
																				// file
																				// name+postfix
								jobj.add(
										"image",
										img.getName().replaceAll(
												"(?mis)" + imagepostfix, ""));
								jobj.add("image_path", img.getName());
								jobj.add("table", table);
								jobj.add("date", Calendar.getInstance()
										.getTime().toString());
								imageData.add(jobj.toString());
							}
						} catch (IOException e) {
							log.info(img.getName() + " is not an Image!");
							e.printStackTrace();
						} catch (Exception e) {
							log.info("Error in Posting to Database.");
							e.printStackTrace();
						}
					}

					// post if > commitsize
					if (imageData.size() >= commitsize || curr == list.length) {
						log.info("Posting to DB @ "
								+ Calendar.getInstance().getTime().toString());
						for (int i = 0; i < numqueries; i++) {
							if (((i + 1) * size) < imageData.size()) {
								fjp.execute(new ImagePost(imageData.subList(
										(i * size), ((i + 1) * size))));
							} else {
								fjp.execute(new ImagePost(imageData.subList(
										(i * size), imageData.size())));
							}
						}

						int w = 0;
						while (fjp.isQuiescent() == false
								&& fjp.getActiveThreadCount() > 0) {
							w++;
						}
						log.info("Waited for " + w + " cycles");
						imageData.clear();
						log.info("Finished Posting to DB @ "
								+ Calendar.getInstance().getTime().toString());
					}
				}
			}

		} else {
			log.error("File Path does Not Exist.Please Check Image Pull!");
		}
		fjp.shutdown();
		fjp = null;
	}// addFromFile

	/**
	 * Called when ready to perform the grab. Expects all options specified in
	 * documentation to be prepared.
	 */
	public void run() {
		if (skipGrab == false) {
			// get images
			log.info("Starting Image Pull @ "
					+ Calendar.getInstance().getTime().toString());
			getImages();
			// some attempts to hint at garbage collection beacuse the program
			// should be able to deal with terabytes and desperation
			System.gc(); // hint only made easier with -X:compactexplicitgc
			Runtime.getRuntime().gc();// hint only made more likely with
										// -X:compactexplicitgc
			log.info("Finished Image Pull @ "
					+ Calendar.getInstance().getTime().toString());
		}

		// add from file if requested
		if (addFromFile) {
			log.info("Adding Images to Database from File @ "
					+ Calendar.getInstance().getTime().toString());
			addFromFile();
			log.info("Finished Adding to Database from File @ "
					+ Calendar.getInstance().getTime().toString());
		}
	}
}
