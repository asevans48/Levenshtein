package com.hygenics.parser;

import java.util.Calendar;
import java.util.List;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunJob {

	private static final Logger log = LoggerFactory.getLogger(MainApp.class);
	private List<String> fpaths;

	private String connname;
	private String jdbhost;
	private String jdbuser;
	private String jdbpass;
	private String jdbdatabase;
	private String jdbport;
	private String jdbname;

	public RunJob() {

	}

	public String getConnname() {
		return connname;
	}

	public void setConnname(String connname) {
		this.connname = connname;
	}

	public String getJdbhost() {
		return jdbhost;
	}

	public void setJdbhost(String jdbhost) {
		this.jdbhost = jdbhost;
	}

	public String getJdbuser() {
		return jdbuser;
	}

	public void setJdbuser(String jdbuser) {
		this.jdbuser = jdbuser;
	}

	public String getJdbpass() {
		return jdbpass;
	}

	public void setJdbpass(String jdbpass) {
		this.jdbpass = jdbpass;
	}

	public String getJdbdatabase() {
		return jdbdatabase;
	}

	public void setJdbdatabase(String jdbdatabase) {
		this.jdbdatabase = jdbdatabase;
	}

	public String getJdbport() {
		return jdbport;
	}

	public void setJdbport(String jdbport) {
		this.jdbport = jdbport;
	}

	public String getJdbname() {
		return jdbname;
	}

	public void setJdbname(String jdbname) {
		this.jdbname = jdbname;
	}

	public List<String> getFpaths() {
		return fpaths;
	}

	public void setFpaths(List<String> fpaths) {
		this.fpaths = fpaths;
	}

	public void run() {

		for (String fpath : fpaths) {
			try {
				KettleEnvironment.init();

				log.info("Starting Pentaho Job kjb PATH: " + fpath + " @ "
						+ Calendar.getInstance().getTime().toString());
				JobMeta jMeta = new JobMeta(fpath, null);
				DatabaseMeta dbm = new DatabaseMeta();
				dbm.setDatabaseInterface(DatabaseMeta
						.getDatabaseInterface("POSTGRESQL"));
				dbm.setName(connname.trim());
				dbm.setHostname(jdbhost);
				dbm.setDBName(jdbdatabase);
				dbm.setDBPort(jdbport);
				dbm.setUsername(jdbuser);
				dbm.setPassword(jdbpass);

				jMeta.addOrReplaceDatabase(dbm);

				Job j = new Job(null, jMeta);

				j.start();
				j.waitUntilFinished();
				if (j.getErrors() > 0) {
					log.info("ERROR IN PENTAHO");
					log.info("Pentaho Job Returned: " + j.getErrors() + " @"
							+ Calendar.getInstance().getTime().toString());
				}

				log.info("Kettle Complete: Cleaning Up");
				jMeta.clear();
				log.info("Pentaho Job Complete @ "
						+ Calendar.getInstance().getTime().toString());

			} catch (KettleException e) {
				e.printStackTrace();
			}
		}

	}

}
