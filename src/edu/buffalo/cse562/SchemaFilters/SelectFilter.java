package edu.buffalo.cse562.SchemaFilters;

import java.util.ArrayList;
import java.util.HashMap;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.FromItem;
import edu.buffalo.cse562.SqlParser.TablesO;


public class SelectFilter extends Filter{
	private ArrayList<ColumnDefinition> inputSchema = null;
	static
	{
		FilterFactorySingleton.getFactoryInstance()
			.getFactory().registerFilter("Select",new SelectFilter());
	}

	public Filter createFilter(){
		return new SelectFilter();
	}

	public void loadSchema(ArrayList<ColumnDefinition> inputSchema){
		this.inputSchema = inputSchema;
	}
	
	public ArrayList<ColumnDefinition> getOutputSchema(){
		return this.inputSchema;
	}
}
