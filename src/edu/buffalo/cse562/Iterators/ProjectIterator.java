package edu.buffalo.cse562.Iterators;

import java.util.Iterator;

import net.sf.jsqlparser.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;

public class ProjectIterator
implements SqlIterator{
 	SqlIterator source;
 	Column[] inputSchema;
 	Expression[] outputExpressions;
 	
	@Override
	public Expression getNext() {
		// TODO Auto-generated method stub
		return null;
	}
 	
 	/*public LeafValue[]  getNext(){
 		//This will call trav function, which will return Operator. Operator in turn has input tuple and output tuple. 
 		
 	}*/
 	
 	
}
