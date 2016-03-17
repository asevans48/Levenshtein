package com.hygenics.parser;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;  

import com.hygenics.scala.ScalaParseDispatcher;
import com.hygenics.scala.BreakMultipleScala;
import com.hygenics.scala.MoveFile;
import com.hygenics.scala.Notification;
import com.hygenics.scala.NumericalChecker;


/**
 * Main Application for Parser. This Parser is Meant to act as a wrapper around
 * Pentaho as well as to enforce quality and speed up parsing.
 * 
 * 
 * Executables can  be run from the tool;e.g. Python scripts
 * 
 * The program achieves speed with the use of ForkJoinPools and Apache Commons
 * latest DBCP which is nearly commercial grade.
 * 
 * 
 * The program does not crawl pages as there are better languages than java for this
 * and my github account has the CrawlerAids for Python 2.7+ and Python3.4+
 * 
 * 
 * Future Incorporations:
 * 
 * Basic Face and Silhouette Recognition with PCA and Eigenfaces
 * 
 * NOTE: Calls to gc() are not reliable but are made because when dealing with
 * such a volume of information, they can have a net benefit if they even work
 * once.
 * 
 * @author aevans
 *
 */
public class MainApp {

	/**
	 * The entire programs main method.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		final Logger log = LoggerFactory.getLogger(MainApp.class);
		log.info("Starting Parse @ " + Calendar.getInstance().getTime().toString()); 
		
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(("file:" + System.getProperty("beansfile").trim()));  
		log.info("Found beans @ " + System.getProperty("beansfile").trim());  
		log.info("Starting"); 
		ArrayList<String> results = null;

		String activity = null;
		log.info("Obtaining Activities"); 
		ActivitiesStack stack = (ActivitiesStack) context.getBean("ActivitiesStack"); 

		// integers keeping track of bean number to pull (e.g. DumpToText0 or
		// DumpToText1)
		// in keeping with the spirit of dumping out data
		int n=0,mv=0,pps=0,sb = 0, kv = 0, cf = 0, zip = 0, bm = 0, dump = 0, kettle = 0, execute = 0, transfer = 0, sqls = 0, job = 0, parsepages = 0, getpages = 0, map = 0, js = 0, sdump = 0, transforms = 0, execs = 0, gim = 0, ems = 0, jd = 0;

		Pattern p = Pattern.compile("[A-Za-z]+[0-9]+"); 
		Matcher m;

		// start stack init
		log.info("Stack Initialized with Size of " + stack.getSize() + " @ " + Calendar.getInstance().getTime().toString());  

		while (stack.getSize() > 0) {
			// pop activity form stack
			activity = stack.Pop();
			log.info("Activities Remaining " + stack.getSize()); 
			m = p.matcher(activity);

			log.info("\n\nACTIVITY: " + activity + "\n\n");  
			if (activity.toLowerCase().contains(
					"manualrepconn")) { 
				log.info("Manual Transformation Started @ " + Calendar.getInstance().getTime().toString()); 
				ManualReplacement mrp = (ManualReplacement) context
						.getBean("ManualRepConn"); 
				mrp.run();
				log.info("Manual Transformation Finished @ " + Calendar.getInstance().getTime().toString()); 
			} else if (activity.toLowerCase().contains(
					"cleanfolder")) { 
				log.info("Folder Cleanup Started @ " + Calendar.getInstance().getTime().toString()); 

				CleanFolder c;
				if (cf == 0
						|| context.containsBean("CleanFolder")) { 
					c = (CleanFolder) ((context.containsBean("CleanFolder")) ? context.getBean("CleanFolder") : context.getBean("CleanFolder" + cf));   
				} else {
					c = (CleanFolder) context.getBean("CleanFolder" + cf); 
				}
				c.run();
				cf++;
				log.info("File Cleanup Complete @ " + Calendar.getInstance().getTime().toString()); 
			} else if (activity.toLowerCase().contains(
					"zip")) { 

				log.info("Zip File Creation Started @ " + Calendar.getInstance().getTime().toString()); 

				Archiver a;
				if (zip == 0
						|| context.containsBean("Zip")) { 
					a = (Archiver) ((context.containsBean("Zip")) ? context.getBean("Zip") : context.getBean("Zip" + zip));   
				} else {
					a = (Archiver) context.getBean("Zip" + zip); 
				}
				a.run();
				zip++;
				log.info("Zip File Creation Complete @ " + Calendar.getInstance().getTime().toString()); 
			} else if (activity.toLowerCase().contains(
					"transfer")) { 
				log.info("File Transfer Started @ " + Calendar.getInstance().getTime().toString()); 

				Upload u;
				if (transfer == 0
						|| context.containsBean("FileTransfer")) { 
					u = (Upload) ((context.containsBean("FileTransfer")) ? context.getBean("FileTransfer") : context.getBean("FileTransfer" + transfer));   
				} else {
					u = (Upload) context.getBean("FileTransfer" + transfer); 
				}
				u.run();
				transfer++;
				log.info("File Transfer Complete @ " + Calendar.getInstance().getTime().toString()); 
			} else if (activity.toLowerCase().contains(
					"droptables")) { 
				// drop tables
				log.info("Dropping Tables @ " + Calendar.getInstance().getTime().toString()); 

				DropTables droptables = (DropTables) context.getBean("DropTables"); 
				droptables.run();

				droptables = null;
				log.info("Done Dropping Tables @ " + Calendar.getInstance().getTime().toString()); 
			} else if (activity.toLowerCase().equals(
					"createtables")) { 
				// create tables
				log.info("Creating Tables @ " + Calendar.getInstance().getTime().toString()); 

				CreateTable create = (CreateTable) context.getBean("CreateTables"); 
				create.run();
				create = null;

				log.info("Done Creating Tables @ " + Calendar.getInstance().getTime().toString()); 
			} else if (activity.toLowerCase().contains(
					"createtableswithreference")) { 
				// create tables
				log.info("Creating Tables @ " + Calendar.getInstance().getTime().toString()); 

				CreateTablesWithReference create = (CreateTablesWithReference) context
						.getBean("CreateTablesWithReference"); 
				create.run();
				create = null;

				log.info("Done Creating Tables @ " + Calendar.getInstance().getTime().toString()); 
			} else if (activity.toLowerCase().contains(
					"truncate")) { 
				// truncate table
				log.info("Truncating @ " + Calendar.getInstance().getTime().toString()); 
				Truncate truncate = (Truncate) context.getBean("Truncate"); 
				truncate.truncate();
				truncate = null;
				log.info("Truncated @ " 
						+ Calendar.getInstance().getTime().toString());
				Runtime.getRuntime().gc();
				log.info("Free Memory: " + Runtime.getRuntime().freeMemory()); 
			} else if (activity.toLowerCase().equals(
					"enforce")) { 
				// enforce schema
				log.info("Enforcing Schema @" + Calendar.getInstance().getTime().toString()); 
				ForceConformity ef = (ForceConformity) context.getBean("EnforceStandards"); 
				ef.run();
				log.info("Done Enforcing Schema @" + Calendar.getInstance().getTime().toString()); 
				Runtime.getRuntime().gc();
				log.info("Free Memory: " + Runtime.getRuntime().freeMemory()); 
			} else if (activity.toLowerCase().contains(
					"enforcewithreference")) { 
				// enforce schema
				ForceConformityWithReference ef = (ForceConformityWithReference) context
						.getBean("EnforceStandardsWithReference"); 
				log.info("Enforcing Schema By Reference @" + Calendar.getInstance().getTime().toString()); 
				ef.run();
				log.info("Done Enforcing Schema @" + Calendar.getInstance().getTime().toString()); 
				Runtime.getRuntime().gc();
				log.info("Free Memory: " + Runtime.getRuntime().freeMemory()); 
			} else if (activity.toLowerCase().trim()
					.equals("repconn")) { 
				log.info("Replacing Transformation Connection Information  @" + Calendar.getInstance().getTime().toString()); 

				RepConn rep = (RepConn) context.getBean("repconn"); 
				rep.run();

				log.info("Finished Replacing Connection Information  @" + Calendar.getInstance().getTime().toString()); 
			} else if (activity.toLowerCase().contains(
					"job")) { 
				// run a Pentaho job as opposed to a Pentaho Transformation
				log.info("Run Job kjb file @" + Calendar.getInstance().getTime().toString()); 

				RunJob kjb = null;

				if (m.find()) {
					kjb = (RunJob) context.getBean(activity);
				} else {
					kjb = (RunJob) context.getBean("Job" + job); 
				}

				kjb.run();
				kjb = null;
				log.info("Pentaho Job Complete @" + Calendar.getInstance().getTime().toString()); 
				Runtime.getRuntime().gc();
			} else if (activity.toLowerCase().compareTo(
					"execute") == 0) { 
				// Execute a process
				log.info("Executing Process @" + Calendar.getInstance().getTime().toString()); 

				if (context.containsBean("Execute") && execs == 0) { 
					ExecuteProcess proc = (ExecuteProcess) context
							.getBean(("Execute")); 
					proc.Execute();
				} else {
					ExecuteProcess proc = (ExecuteProcess) context
							.getBean(("Execute" + execute)); 
					proc.Execute();
				}

				execs++;
				log.info("Pages Obtained @" + Calendar.getInstance().getTime().toString()); 
				Runtime.getRuntime().gc();
				log.info("Free Memory: " + Runtime.getRuntime().freeMemory()); 
			}else if(activity.toLowerCase().contains("parsepageswithscala") || activity.toLowerCase().contains("parsepagesscala")){
				//parse pages with scala
				log.info("Parsing Pages with Scala @ "+Calendar.getInstance().getTime().toString());
				ScalaParseDispatcher pds=null;
				if(context.containsBean("ParsePagesScala"+pps)){
					pds=(ScalaParseDispatcher) context.getBean("ParsePagesScala"+pps);
				}else if(context.containsBean("ParsePagesScala") && pps > 0){
					pds = (ScalaParseDispatcher) context.getBean("ParsePagesScala");
				}
				pps++;
				pds.run();
				Runtime.getRuntime().gc();
				log.info("Finished Parsing Pages with Scala @ "+Calendar.getInstance().getTime().toString());
			}else if (activity.toLowerCase().contains("parsepages")) { 
			
				// Parse Pages with java
				log.info("Parsing Individual Pages  @" + Calendar.getInstance().getTime().toString()); 
				if (context.containsBean("ParsePages") && parsepages == 0) { 
					ParseDispatcher pd = (ParseDispatcher) context
							.getBean("ParsePages"); 
					pd.run();
					pd = null;
				} else {
					ParseDispatcher pd = (ParseDispatcher) context
							.getBean("ParsePages" + parsepages); 
					pd.run();
					pd = null;
				}

				parsepages++;
				log.info("Finished Parsing @" + Calendar.getInstance().getTime().toString()); 
				Runtime.getRuntime().gc();
				log.info("Free Memory: " + Runtime.getRuntime().freeMemory()); 
			} else if (activity.toLowerCase().contains(
					"kvparser")) { 
				// Parse Pages using the KV Parser
				log.info("Parsing Individual Pages with Key Value Pairs  @" + Calendar.getInstance().getTime().toString()); 
				if (context.containsBean("KVParser") && parsepages == 0) { 
					KVParser pd = (KVParser) context.getBean("KVParser"); 
					pd.run();
					pd = null;
				} else {
					KVParser pd = (KVParser) context.getBean("KVParser" + kv); 
					pd.run();
					pd = null;
				}

				parsepages++;
				log.info("Finished Parsing @" + Calendar.getInstance().getTime().toString()); 
				Runtime.getRuntime().gc();
				log.info("Free Memory: " + Runtime.getRuntime().freeMemory()); 
			} else if (activity.toLowerCase().contains("parsewithsoup")) { 

				// Parse Pages with Jsoup
				log.info("Parsing Pages with JSoup @ " + Calendar.getInstance().getTime().toString()); 

				if (context.containsBean("ParseJSoup") && js == 0) { 
					ParseJSoup psj = (ParseJSoup) context.getBean("ParseJSoup"); 
					psj.run();
				} else {
					ParseJSoup psj = (ParseJSoup) context.getBean("ParseJSoup" + Integer.toString(js)); 
					psj.run();
				}
				js++;
				log.info("Finished Parsing @" + Calendar.getInstance().getTime().toString()); 
				Runtime.getRuntime().gc();
				log.info("Finished Parsing with JSoup @ " + Calendar.getInstance().getTime().toString()); 
			} else if (activity.toLowerCase().contains("breakmultiplescala") || activity.toLowerCase().contains("breakmultiplewithscala")){ 
				log.info("Breaking Records"); 
				BreakMultipleScala bms = null;
				if (context
						.containsBean("BreakMultipleScala" + ems)) { 
					bms = (BreakMultipleScala) context.getBean("BreakMultipleScala" + sb); 
				} else {
					bms = (BreakMultipleScala) context.getBean("BreakMultipleScala"); 
				}
				bms.run();
				bms = null;
				sb++;
				Runtime.getRuntime().gc();
				System.gc();
				log.info("Free Memory: " + Runtime.getRuntime().freeMemory()); 
				log.info("Completed Breaking Tasks"); 
			} else if (activity.toLowerCase().contains("breakmultiple")) { 

				// break apart multi-part records
				log.info("Breaking apart Records (BreakMultiple) @" + Calendar.getInstance().getTime().toString()); 

				if (context.containsBean("BreakMultiple") && bm == 0) { 
					BreakMultiple br = (BreakMultiple) context
							.getBean(("BreakMultiple")); 
					br.run();
					br = null;
				} else {
					BreakMultiple br = (BreakMultiple) context
							.getBean(("BreakMultiple" + Integer.toString(bm))); 
					br.run();
					br = null;
				}
				bm++;

				log.info("Finished Breaking Apart Records @" 
						+ Calendar.getInstance().getTime().toString());
				log.info("Free Memory: " + Runtime.getRuntime().freeMemory()); 
			} else if (activity.toLowerCase().compareTo("mapper") == 0) { 
				// remap data
				log.info("Mapping Records @" + Calendar.getInstance().getTime().toString()); 

				if (context.containsBean("Mapper") && map == 0) { 
					Mapper mp = (Mapper) context.getBean("Mapper"); 
					mp.run();
					mp = null;
				} else {
					Mapper mp = (Mapper) context.getBean("Mapper" + Integer.toString(map)); 
					mp.run();
					mp = null;
				}
				map++;
				log.info("Completed Mapping Records @ " + Calendar.getInstance().getTime().toString()); 
			} else if (activity.toLowerCase().contains(
					"getimages")) { 
				// Get Images in a Separate Step
				log.info("Beggining Image Pull @ " + Calendar.getInstance().getTime().toString()); 

				if (context.containsBean("getImages") && gim == 0) { 
					GetImages gi = (GetImages) context.getBean("getImages"); 
					gi.run();
					log.info("Image Pull Complete @ " + Calendar.getInstance().getTime().toString()); 
					gi = null;
				} else {
					GetImages gi = (GetImages) context.getBean("getImages"); 
					gi.run();
					log.info("Image Pull Complete @ " + Calendar.getInstance().getTime().toString()); 
					gi = null;
				}
				gim++;
				Runtime.getRuntime().gc();
				System.gc();
				log.info("Free Memory: " + Runtime.getRuntime().freeMemory()); 

			} else if (activity.toLowerCase().compareTo(
					"sql") == 0) { 
				// execute a sql command
				log.info("Executing SQL Query @ " + Calendar.getInstance().getTime().toString()); 

				if (context.containsBean("SQL") && sqls == 0) { 
					ExecuteSQL sql;

					if (m.find()) {
						sql = (ExecuteSQL) context.getBean(activity);
					} else {
						sql = (ExecuteSQL) context.getBean("SQL"); 
					}

					sql.execute();
					sql = null;
				} else {

					ExecuteSQL sql;

					if (m.find()) {
						sql = (ExecuteSQL) context.getBean(activity);
					} else {
						sql = (ExecuteSQL) context.getBean("SQL" + sqls); 
					}

					sql.execute();
					sql = null;
				}

				sqls++;

				log.info("Finished SQL Query @ " + Calendar.getInstance().getTime().toString()); 
				Runtime.getRuntime().gc();
				System.gc();
				log.info("Free Memory: " + Runtime.getRuntime().freeMemory()); 
			} else if (activity.toLowerCase().compareTo(
					"kettle") == 0) { 
				// run one or more kettle transformation(s)
				log.info("Beginning Kettle Transformation @ " + Calendar.getInstance().getTime().toString()); 
				RunTransformation rt = null;

				if (context.containsBean("kettle") && transforms == 0) { 
					if (m.find()) {
						rt = (RunTransformation) context.getBean(activity);
					} else {
						rt = (RunTransformation) context.getBean(("kettle")); 
					}

					rt.run();
					rt = null;
				} else {
					if (m.find()) {
						rt = (RunTransformation) context.getBean(activity);
					} else {
						rt = (RunTransformation) context.getBean(("kettle" + kettle)); 
					}

					rt.run();
					rt = null;
				}
				transforms++;
				log.info("Ending Kettle Transformation @ " + Calendar.getInstance().getTime().toString()); 
				Runtime.getRuntime().gc();
				System.gc();
				log.info("Free Memory: " + Runtime.getRuntime().freeMemory()); 
				kettle++;
			} else if (activity.toLowerCase().contains(
					"dumptotext")) { 
				// dump to a text file via java
				log.info("Dumping to Text @ " + Calendar.getInstance().getTime().toString()); 

				DumptoText dtt = null;
				if (m.find()) {
					dtt = (DumptoText) context.getBean(activity);
				} else {
					dtt = (DumptoText) context.getBean("DumpToText" + dump); 
				}

				dtt.run();
				dump++;
				log.info("Completed Dump @ " + Calendar.getInstance().getTime().toString()); 
				dtt = null;
				Runtime.getRuntime().gc();
				System.gc();
				log.info("Free Memory: " + Runtime.getRuntime().freeMemory()); 
			} else if (activity.toLowerCase().equals(
					"jdump")) { 
				log.info("Dumping via JDump @ " + Calendar.getInstance().getTime().toString()); 
				if (jd == 0
						&& context.containsBean("JDump")) { 
					JDump j = (JDump) context.getBean("JDump"); 
					jd++;
					j.run();
				} else {
					JDump j = (JDump) context.getBean("JDump" + jd); 
					jd++;
					j.run();
				}
				Runtime.getRuntime().gc();
				System.gc();
				log.info("Finished Dumping via JDump @ " + Calendar.getInstance().getTime().toString()); 
			} else if (activity.toLowerCase().contains(
					"jdumpwithreference")) { 
				log.info("Dumping via JDump @ " + Calendar.getInstance().getTime().toString()); 
				if (jd == 0
						&& context.containsBean("JDumpWithReference")) { 
					JDumpWithReference j = (JDumpWithReference) context
							.getBean("JDumpWithReference"); 
					jd++;
					j.run();
				} else {
					JDumpWithReference j = (JDumpWithReference) context
							.getBean("JDumpWithReference" + jd); 
					jd++;
					j.run();
				}
				Runtime.getRuntime().gc();
				System.gc();
				log.info("Finished Dumping via JDump @ " + Calendar.getInstance().getTime().toString()); 
			} else if (activity.toLowerCase().compareTo(
					"commanddump") == 0) { 

				// dump to text using a client side sql COPY TO command
				log.info("Dumping via SQL @ " + Calendar.getInstance().getTime().toString()); 
				CommandDump d = (CommandDump) context.getBean("dump"); 
				d.run();
				d = null;
				log.info("Completed Dump @ " + Calendar.getInstance().getTime().toString()); 
				// most likely not needed by satisfies my paranoia
				Runtime.getRuntime().gc();
				System.gc();
			} else if (activity.toLowerCase().equals(
					"specdump")) { 
				// Specified Dump
				log.info("Dumping via Specified Tables, Files, and Attributes @ " + Calendar.getInstance().getTime().toString()); 

				if (context.containsBean("SpecDump") && sdump == 0) { 
					sdump++;
					SpecifiedDump sd = (SpecifiedDump) context.getBean("SpecDump"); 
					sd.run();
					sd = null;
				} else if (context.containsBean("SpecDump" + Integer.toString(sdump))) { 
					SpecifiedDump sd = (SpecifiedDump) context
							.getBean("SpecDump" + Integer.toString(sdump)); 
					sd.run();
					sd = null;
				}
				sdump++;
				log.info("Completed Dumping via Specified Tables, Files, and Attributes @ " + Calendar.getInstance().getTime().toString()); 
			} else if (activity.toLowerCase().contains(
					"specdumpwithreference")) { 
				// Specified Dump
				log.info("Dumping via Specified Tables, Files, and Attributes by Reference @ " + Calendar.getInstance().getTime().toString()); 

				if (context.containsBean("SpecDumpWithReference") && sdump == 0) { 
					sdump++;
					SpecDumpWithReference sd = (SpecDumpWithReference) context
							.getBean("SpecDumpWithReference"); 
					sd.run();
					sd = null;
				} else if (context.containsBean("SpecDumpWithReference" + Integer.toString(sdump))) { 
					SpecDumpWithReference sd = (SpecDumpWithReference) context
							.getBean("SpecDumpWithReference" + Integer.toString(sdump)); 
					sd.run();
					sd = null;
				} else {
					log.info("Bean Not Found For " + activity); 
				}
				sdump++;
				log.info("Completed Dumping via Specified Tables, Files, and Attributes @ " + Calendar.getInstance().getTime().toString()); 
			} else if (activity.toLowerCase().compareTo(
					"email") == 0) { 
				// email completion notice
				log.info("Sending Notice of Completion @ " + Calendar.getInstance().getTime().toString()); 

				if (context.containsBean("Email") && ems == 0) { 
					Send s = (Send) context.getBean("Email"); 
					s.run();
					s = null;
				} else {
					Send s = (Send) context.getBean("Email" + Integer.toString(ems)); 
					s.run();
					s = null;
				}
				ems++;
				Runtime.getRuntime().gc();
				System.gc();
				log.info("Completed Email @ " + Calendar.getInstance().getTime().toString()); 
				log.info("Free Memory: " + Runtime.getRuntime().freeMemory()); 
			} else if (activity.toLowerCase().equals(
					"qa")) { 
				// perform qa
				log.info("Performing Quality Assurance @ " + Calendar.getInstance().getTime().toString()); 

				if (context.containsBean("QA")) { 
					QualityAssurer qa = (QualityAssurer) context
							.getBean("QA"); 
					qa.run();
					qa = null;
				}

				// attempt to hint --> all tasks are really intense so anything
				// is nice
				Runtime.getRuntime().gc();
				System.gc();
				log.info("Completed QA @ " + Calendar.getInstance().getTime().toString()); 
				log.info("Free Memory: " + Runtime.getRuntime().freeMemory()); 
			} else if (activity.toLowerCase().equals("notify")) { 
				log.info("Running Notification Tasks"); 
				Notification nt=null;
				if (context.containsBean("Notify"+Integer.toString(n))) { 
					nt = (Notification) context.getBean("Notify"+Integer.toString(n)); 
				}else{
					nt= (Notification) context.getBean("Notify");
				}
				
				nt.run();
				nt = null;
				n++;
				
				if (context.containsBean("Email") && ems == 0) { 
					Send s = (Send) context.getBean("Email"); 
					s.run();
					s = null;
				} else if (context.containsBean("Email" + ems)) { 
					Send s = (Send) context.getBean("Email" + Integer.toString(ems)); 
					s.run();
					s = null;
				}

				ems++;
				Runtime.getRuntime().gc();
				System.gc();
				log.info("Free Memory: " + Runtime.getRuntime().freeMemory()); 
				log.info("Completed Notification Tasks"); 
			}else if(activity.toLowerCase().contains("move")){
				log.info("Moving Files @ "+Calendar.getInstance().getTime().toString());
				
				MoveFile mf=null;
				if(context.containsBean("Move"+Integer.toString(mv))){
					mf=(MoveFile) context.getBean("Move"+Integer.toString(mv));
				}else{
					mf=(MoveFile) context.getBean("Move");
				}
				mf.run();
				mv++;
				Runtime.getRuntime().gc();
				log.info("Finished Moving Files @ "+Calendar.getInstance().getTime().toString());
			}else if(activity.toLowerCase().contains("numericalcheck")){
				log.info("Checking Counts");
				
				NumericalChecker nc = (NumericalChecker) context.getBean("NumericalChecker");
				nc.run();
				Runtime.getRuntime().gc();
				
				log.info("Finished Checking Counts @ "+Calendar.getInstance().getTime().toString());
			}else {
				log.info("Activity " + activity + " does not exist!");  
			}

		}

		log.info("Completed Parse @ " + Calendar.getInstance().getTime().toString()); 
		context.destroy();
		context.close();
	}

}
