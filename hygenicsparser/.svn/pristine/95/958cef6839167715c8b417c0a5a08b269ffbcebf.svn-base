package com.hygenics.parser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleMissingPluginsException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.trans.TransMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * Takes in a list of transformation files and jobs and edits the connection
 * information using a series of properties and a conname.
 * 
 * Required Properties
 * 
 * -jdbpass (password) -jdbhost -jdbuser -jdbdatabase -jdbname
 * 
 * @author asevans
 *
 */
public class RepConn {

	private Logger log = LoggerFactory.getLogger(MainApp.class);
	private List<String> transformations;
	private String connname;
	private String jdbhost;
	private String jdbuser;
	private String jdbpass;
	private String jdbdatabase;
	private String jdbport;
	private String jdbname;
	private List<String> transformdirs;
	private String transformid = ".ktr";

	public RepConn() {

	}

	public List<String> getTransformdirs() {
		return transformdirs;
	}

	public void setTransformdirs(List<String> transformdirs) {
		this.transformdirs = transformdirs;
	}

	public String getTransformid() {
		return transformid;
	}

	public void setTransformid(String transoformid) {
		this.transformid = transoformid;
	}

	public List<String> getTransformations() {
		return transformations;
	}

	public void setTransformations(List<String> transformations) {
		this.transformations = transformations;
	}

	public String getConnname() {
		return connname;
	}

	/**
	 * Sets the database connection name: the top bar in Pentaho that asks you
	 * to name the connection
	 * 
	 * @param connname
	 */
	@Required
	public void setConnname(String connname) {
		this.connname = connname;
	}

	public String getJdbhost() {
		return jdbhost;
	}

	@Required
	public void setJdbhost(String jdbhost) {
		this.jdbhost = jdbhost;
	}

	public String getJdbuser() {
		return jdbuser;
	}

	@Required
	public void setJdbuser(String jdbuser) {
		this.jdbuser = jdbuser;
	}

	public String getJdbpass() {
		return jdbpass;
	}

	@Required
	public void setJdbpass(String jdbpass) {
		this.jdbpass = jdbpass;
	}

	public String getJdbdatabase() {
		return jdbdatabase;
	}

	@Required
	public void setJdbdatabase(String jdbdatabase) {
		this.jdbdatabase = jdbdatabase;
	}

	public String getJdbport() {
		return jdbport;
	}

	@Required
	public void setJdbport(String jdbport) {
		this.jdbport = jdbport;
	}

	public String getJdbname() {
		return jdbname;
	}

	@Required
	public void setJdbname(String jdbname) {
		this.jdbname = jdbname;
	}

	/**
	 * Gets the Files from the Individual Directories
	 * 
	 * @param dir
	 * @return
	 */
	private ArrayList<String> getFiles(String dir) {
		ArrayList<String> retarr = new ArrayList<String>();

		if (new File(dir).list().length == 0) {
			return retarr;
		}

		for (String pf : new File(dir).list()) {
			if (new File(pf).isDirectory()) {
				retarr.addAll(getFiles(dir + "/" + pf));
			} else if (new File(pf).isFile() && pf.contains(transformid)) {
				retarr.add(dir + "/" + pf);
			}
		}

		return retarr;
	}

	/**
	 * Run the Pentaho Transformation Cleaner that replaces the connection
	 * information for the specified transformations
	 * 
	 * @throws -NullPointerException when no transformation list given
	 */
	public void run() {

		// save it

		log.info("Starting RepConn");
		if (transformations == null && transformdirs == null) {
			try {
				throw new NullPointerException(
						"No transformation files specified!");
			} catch (NullPointerException e) {
				e.printStackTrace();
			}
		} else if (transformdirs != null) {
			transformations = new ArrayList<String>();

			for (String dir : transformdirs) {
				transformations.addAll(getFiles(dir));
			}
		}

		if (transformations != null && transformations.size() > 0) {
			// vars
			TransMeta transMeta;
			DatabaseMeta dbm;
			JobMeta jMeta;

			/**
			 * Iterate through the provided list of transformations and perform
			 * the requested update
			 */
			for (String trfpath : transformations) {
				log.info("Manipulating: " + trfpath + " @ "
						+ Calendar.getInstance().getTime());
				transMeta = null;
				dbm = null;
				jMeta = null;

				File f = new File(trfpath);

				if (f.exists()) {

					try {
						KettleEnvironment.init();
						transMeta = new TransMeta(trfpath);

						if (trfpath.toLowerCase().contains(".ktr")) {
							// change transformation database information
							List<DatabaseMeta> dbs = transMeta.getDatabases();
							int pos = 0;
							while (pos < dbs.size()
									&& dbs.get(pos).getName()
											.compareTo(connname.trim()) != 0) {
								log.info("Conn FOUND :"
										+ dbs.get(pos).getName());
								pos++;
							}

							if (pos < dbs.size()
									&& dbs.get(pos).getName()
											.compareTo(connname.trim()) == 0) {
								dbm = new DatabaseMeta();
								dbm.setDatabaseInterface(DatabaseMeta
										.getDatabaseInterface("POSTGRESQL"));
								dbm.setName(connname.trim());
								dbm.setHostname(jdbname.trim());
								dbm.setDBName(jdbdatabase.trim());
								dbm.setDBPort("5432");
								dbm.setUsername(jdbuser.trim());
								dbm.setPassword(jdbpass.trim());

								// save transformation
								transMeta.addOrReplaceDatabase(dbm);
								transMeta.setChanged(true);
								transMeta.saveSharedObjects();

								String xml = transMeta.getXML();
								xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
										+ xml;
								transMeta.saveSharedObjects();
								log.info("Saving to " + trfpath);
								try (FileWriter writer = new FileWriter(
										new File(trfpath))) {
									writer.write(xml);
								}

							}
						} else if (trfpath.contains(".kjb")) {

							// change job database information
							jMeta = new JobMeta(trfpath, null);
							dbm = new DatabaseMeta();
							dbm.setDatabaseInterface(DatabaseMeta
									.getDatabaseInterface("POSTGRESQL"));
							dbm.setName(connname.trim());
							dbm.setHostname(jdbhost.trim());
							dbm.setDBName(jdbdatabase.trim());
							dbm.setDBPort(jdbport.trim());
							dbm.setUsername(jdbuser.trim());
							dbm.setPassword(jdbpass.trim());

							// save job
							jMeta.addOrReplaceDatabase(dbm);
							jMeta.setChanged(true);
							jMeta.saveSharedObjects();

							String xml = jMeta.getXML();
							xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
									+ xml;
							try (FileWriter writer = new FileWriter(new File(
									trfpath))) {
								writer.write(xml);
							}

						} else {
							// this will hit if the file is not a .kjb or.ktr
							log.warn("SKIP FILE WARNING: "
									+ trfpath
									+ " Is Not a Proper or Recognized Pentaho File!\n The extensions .ktr and .kjb are accepted!\n");
						}

					} catch (KettleXMLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (KettleMissingPluginsException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (KettleDatabaseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (KettleException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else {
					try {
						throw new FileNotFoundException("File Not Found");
					} catch (FileNotFoundException e) {
						e.printStackTrace();
					}
				}

				log.info("Done!\nGetting next Fpath");
			}
			log.info("Ending Repconn");
		}

	}
}
