package edu.buffalo.cse562.Operators;

import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.expression.*;

public interface Operator {
	//HashMap<String,ColumnDefinition> inputSchema = null;
	//HashMap<String,ColumnDefinition> outputSchema = null;
	
	//ArrayList<ColumnDefinition> inputSchema = null;
	HashMap<String, Integer> inputSchema = null;

	//HashMap<String,Expression> tuple =null;
	Expression[] tuple = null;
        
    public HashMap<String, Integer> getOutputSchema();
	
    public Expression[] getTuple();
	
	public void reset();
}
