package com.hygenics.parser;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Execuutes a process using process builder. Takes in a map with the process
 * name and what should be either a one argument string or string like array to
 * processed (1 parameter only)
 * 
 * @author aevens
 *
 */
public class ExecuteProcess {

	Logger log = LoggerFactory.getLogger(MainApp.class);
	private Map<String, String> processes;

	public void ExecuteProcess() {

	}

	public Map<String, String> getProcesses() {
		return processes;
	}

	public void setProcess(Map<String, String> processes) {
		this.processes = processes;
	}

	/**
	 * Redirects the Standar Error and Standard Output to SLF4j logger
	 * 
	 * @author aevens
	 *
	 */
	private class RedirectStream {
		private InputStream is;
		private boolean run = true;

		public RedirectStream(InputStream is) {
			this.is = is;
		}

		public boolean isRun() {
			return run;
		}

		public void setRun(boolean run) {
			this.run = run;
		}

		/**
		 * Redirect the output Here
		 */
		private void redirect() {
			InputStreamReader isr = new InputStreamReader(is);
			int ch = -1;
			StringBuilder builder = new StringBuilder();

			do {
				try {
					if (isr.ready()) {
						while ((ch = isr.read()) != -1) {
							builder.append((char) ch);
						}

						log.info(builder.toString());
						builder = new StringBuilder();
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} while (run);
		}

		public void read() {
			redirect();
		}
	}

	/**
	 * Execute the Process
	 */
	private void run() {
		RedirectStream rd = null;
		InputStream procStream = null;
		if (processes != null) {
			try {

				// executes processes serially
				for (String proc : processes.keySet()) {
					log.info("Starting Process: " + proc);

					ProcessBuilder pb;
					if (processes.get(proc).length() != 0) {
						pb = new ProcessBuilder(proc, processes.get(proc));
					} else {
						pb = new ProcessBuilder(proc);
					}

					Process p = pb.start();

					pb.redirectErrorStream(true);
					pb.redirectOutput();
					rd = new RedirectStream(p.getInputStream());

					p.waitFor();
					rd.setRun(false);

				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * Called to start executing processes
	 */
	public void Execute() {
		run();
	}
}
