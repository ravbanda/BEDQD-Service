package com.deloitte.bedqd.model;

import java.util.List;
import java.util.Map;

public class OpenDQPrioritySummary {
	
	private Map<String,List<OpenDQPrioritySummaryDetail>> penDQPrioritySummaryMap;

	public Map<String, List<OpenDQPrioritySummaryDetail>> getPenDQPrioritySummaryMap() {
		return penDQPrioritySummaryMap;
	}

	public void setPenDQPrioritySummaryMap(Map<String, List<OpenDQPrioritySummaryDetail>> penDQPrioritySummaryMap) {
		this.penDQPrioritySummaryMap = penDQPrioritySummaryMap;
	}
	
	
 
}
