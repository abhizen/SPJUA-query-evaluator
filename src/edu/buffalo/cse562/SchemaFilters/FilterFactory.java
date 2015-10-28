package edu.buffalo.cse562.SchemaFilters;

import java.util.HashMap;

public class FilterFactory {
	HashMap<String,Filter > m_registeredFilters 
		= new HashMap<String,Filter>();
	public void registerFilter(String filterName, Filter f){
		m_registeredFilters.put(filterName, f);
	}
	
	public Filter createFilter(String filterName){
		
		return (Filter)m_registeredFilters.get(filterName);
	}
}
abstract class Filter{
	public abstract Filter createFilter();
}