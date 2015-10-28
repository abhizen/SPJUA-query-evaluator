package edu.buffalo.cse562.SchemaFilters;

public class FilterFactorySingleton {
	static FilterFactorySingleton filterFactory = null;
	public static FilterFactorySingleton getFactoryInstance(){
		if(filterFactory == null)
			filterFactory = new FilterFactorySingleton();
		return filterFactory;
	}
	
	public FilterFactory getFactory(){
		FilterFactory factory = new FilterFactory();
		return factory;
	}
	
}
