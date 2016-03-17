package com.hygenics.parser;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class adds files to a zip file recursively based on the base directory
 * and zips to a specified directory. If requested that directory can then be
 * deleted using delDir=true.
 * 
 * Zipped files can be deleted by specifying delFiles as true.
 * 
 * The default for deletions is false.
 * 
 * A date stamp is automatically added but this can be changed to have no
 * datestamp with addDateStamp.
 * 
 * To avoid Files of a certain type, add the extension to the avoidanceString in
 * a manner such as '.zip;.jpg,.JPG,.jpeg'
 * 
 * @author aevans
 *
 */
public class Archiver {

	private static final Logger log = LoggerFactory.getLogger(MainApp.class);

	private String avoidanceString;
	private String zipDirectory;
	private String zipFile;
	private String basedirectory;
	private boolean delFiles = false;
	private boolean delDir = false;
	
	//set combo
	private boolean clean = true; //if datestamps are present, specify to clean
	private boolean addDateStamp = true; //add datestamps to zips
	private int maxZips = 2; //maximum number of zip folders to keep in the system
	
	public int getMaxZips() {
		return maxZips;
	}

	public void setMaxZips(int maxZips) {
		this.maxZips = maxZips;
	}

	public String getAvoidanceString() {
		return avoidanceString;
	}

	public void setAvoidanceString(String avoidanceString) {
		this.avoidanceString = avoidanceString;
	}

	public boolean isAddDateStamp() {
		return addDateStamp;
	}

	public void setAddDateStamp(boolean addDateStamp) {
		this.addDateStamp = addDateStamp;
	}

	public String getZipDirectory() {
		return zipDirectory;
	}

	public void setZipDirectory(String zipDirectory) {
		this.zipDirectory = zipDirectory;
	}

	public String getZipFile() {
		return zipFile;
	}

	public void setZipFile(String zipFile) {
		this.zipFile = zipFile;
	}

	public String getBasedirectory() {
		return basedirectory;
	}

	public void setBasedirectory(String basedirectory) {
		this.basedirectory = basedirectory;
	}

	public boolean isDelFiles() {
		return delFiles;
	}

	public void setDelFiles(boolean delFiles) {
		this.delFiles = delFiles;
	}

	public boolean isDelDir() {
		return delDir;
	}

	public void setDelDir(boolean delDir) {
		this.delDir = delDir;
	}

	private void zip(String dir, String zdir, URI zipURI, FileSystem zfs) {
		if (new File(dir).exists()) {
			try {
				for (String f : new File(dir).list()) {
					File fp = new File(dir + f);

					if (!avoidanceString.contains(f
							.replaceAll(".*?(?=\\.)", ""))) {
						if (fp.isFile()) {
							Path extPath = Paths.get(dir + f);
							Path zipPath = zfs.getPath(zdir + f);

							if (delFiles) {
								log.info("Deleting File with Zip: " + dir + f);
								Files.copy(extPath, zipPath,StandardCopyOption.REPLACE_EXISTING);
								new File(dir + f).delete();
							} else {
								log.info("Copying File to Zip File: " + dir + f);
								Files.copy(extPath, zipPath,StandardCopyOption.REPLACE_EXISTING);
							}
						} else if (fp.isDirectory()) {
							zip(dir + f, zdir + f, zipURI, zfs);
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}// zip
	
	/**
	 * The Comparator for sorting the array list
	 * @author aevans
	 *
	 */
	private class StringCompare implements Comparator<String>{

		@Override
		public int compare(String o1, String o2) {
			return o1.compareTo(o2);
		}
		
	}

	private void zip() {
		if (new File(zipDirectory).exists()) {
			Map<String, Object> env = new HashMap<String, Object>(2);
			env.put("create", "true");
			env.put("useTempFile", Boolean.TRUE);
			
			String dtstmp = ((addDateStamp) ? Calendar.getInstance().getTime().toString().replaceAll("[\\s\\t\\r\\n]+", ""):"");
			
			if(addDateStamp){ //clean up folder if date stamp was specified
				log.info("DELETING OLD ZIP FILES");
				File f = new File(this.zipDirectory);
				String[] fileList = f.list();
				ArrayList<String> cleanFiles = new ArrayList<String>(fileList.length);
				for(int i = 0; i < fileList.length; i++){
					if(fileList[i].toLowerCase().contains(".zip")){
						cleanFiles.add(fileList[i]);
					}
				}
				
				if(cleanFiles.size() > maxZips){
					//sort
					cleanFiles.sort(new StringCompare());
					
					//delete past and including maxZips index
					for(String path : cleanFiles.subList(maxZips,cleanFiles.size())){
						log.info("DELETING: "+path);
						new File(this.zipDirectory,path).delete();
					}
				}
			}
			
			
			URI uri = URI.create("jar:file:"+ this.zipDirectory+ this.zipFile.replace(".zip", "")+dtstmp+".zip");
			
			
			try (FileSystem zfs = FileSystems.newFileSystem(uri, env)) {
				zip(basedirectory, "/", uri, zfs);
			} catch (IOException e) {
				e.printStackTrace();
			}

			if (delDir) {
				log.info("Deleting Directory: " + basedirectory);
				new File(basedirectory).delete();
			}
		} else {
			log.error("Zip Directory: " + zipDirectory + " Does not Exist!");
		}

	}// zip helper

	/**
	 * Run the Zip File Creator.
	 */
	public void run() {
		log.info("Starting Zip Creation @ "+ Calendar.getInstance().getTime().toString());
		zip();
		log.info("Finished Zipping Files @ "+ Calendar.getInstance().getTime().toString());
	}// run
}
