package com.deloitte.bedqd.controller;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.deloitte.bedqd.model.InternalControlsModel;
import com.deloitte.bedqd.service.DataQualityService;
import com.deloitte.bedqd.service.InternalControlService;

@RestController
public class DataQualityController {

	@Autowired
	DataQualityService service;
	
	@Autowired
	InternalControlService internalControlService;
	
	@RequestMapping(value="/dataQualityMonitoring", method = RequestMethod.GET)
	public Object getDataQualityEndPoint(){
		return service.getDQMonitoringData();
	}
	
	@RequestMapping(value="/keyHighlights", method = RequestMethod.GET)
	public Object getKeyHighlightsEndPoint(){
		//@RequestParam("startDate") String startDate,@RequestParam("endDate") String endDate
		Date startDateRange = null;
		Date endDateRange = null;
		try 
		{
			SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy");
			startDateRange=format.parse("01/01/2010");
			endDateRange=format.parse("28/01/2018");
			
			System.out.println("Inside getKeyHighlightsEndPoint :: start date " + startDateRange + "  end date  " + endDateRange);
		
		} catch (ParseException e) {
			System.out.println("Error occured in getKeyHighlightsEndPoint");
			e.printStackTrace();
		}
		return service.getKeyHighlightsData(startDateRange,endDateRange);
	}
	
	@RequestMapping(value="/measureRemediateDataQuality", method = RequestMethod.GET)
	public Object getMeasureRemediateDQEndPoint(){
		return service.getMeasureRemediateDQData();
	}
	
	@RequestMapping(value="/internalControlsEndPoint", method = RequestMethod.GET)
	public InternalControlsModel getInternalControlsEndPoint(){
		return internalControlService.getInternalControlsData();
	}
}


