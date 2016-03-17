package com.hygenics.parser;

/**
 * An activities stack. Rather than initiate all beans at once with the giant reference bean, this is preferred.
 * Memory usage on this program is very high.
 * 
 * @author asevans
 */

import java.util.List;

public class ActivitiesStack {

	private List<String> activities;

	public ActivitiesStack() {

	}

	public List<String> getActivities() {
		return activities;
	}

	public void setActivities(List<String> activities) {
		this.activities = activities;

	}

	public String Pop() {
		String data = activities.get(0);
		activities.remove(0);
		return data;
	}

	public void Push(String activity) {
		activities.add(0, activity);
	}

	public int getSize() {
		return activities.size();
	}

}
