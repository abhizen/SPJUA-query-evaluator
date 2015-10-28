package edu.buffalo.cse562.RATree;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.FromItem;
import edu.buffalo.cse562.Operators.BigDataScanOperator;
import edu.buffalo.cse562.Operators.ExtendedProjectionOperator;

import edu.buffalo.cse562.Operators.LimitOperator;
import edu.buffalo.cse562.Operators.Operator;
import edu.buffalo.cse562.Operators.OrderByOperator;

import edu.buffalo.cse562.Operators.SelectionOperator;
import edu.buffalo.cse562.SqlParser.TablesO;
import edu.buffalo.cse562.SchemaFilters.*;

public class SchemaCalculator {
	   private String operation;
	    private HashMap<String,Expression>  tuple = new HashMap<>();
	    private ArrayList<HashMap<String,Expression>> tuples=new ArrayList<>();
	    private FilterFactorySingleton filterFactoryInstance = null; 
    	private FilterFactory filterFactory  = null;
    	private ScanFilter scanFilter = null;
    	private ProjectionFilter projFilter = null;
    	private JoinFilter joinFilter = null;
    	private SelectFilter selectFilter = null;
    	private ArrayList<ColumnDefinition> resultSchema = null;
	    
	   public SchemaCalculator(){
	    	filterFactoryInstance 	= FilterFactorySingleton.getFactoryInstance();
			filterFactory  = filterFactoryInstance.getFactory();
			filterFactory.registerFilter("Scan", new ScanFilter());
			scanFilter = (ScanFilter) (filterFactory.createFilter("Scan"));
			filterFactory.registerFilter("Projection", new ProjectionFilter());
			projFilter = (ProjectionFilter) (filterFactory.createFilter("Projection"));
			filterFactory.registerFilter("Join", new JoinFilter());
			joinFilter = (JoinFilter) (filterFactory.createFilter("Join"));
			filterFactory.registerFilter("Select", new SelectFilter());
			selectFilter = (SelectFilter)(filterFactory.createFilter("Select"));
	    }

	    public void trav(Node n, HashMap<String, TablesO> parsedTables) {
	        if (n == null) {
	            return;
	        }

	        trav(n.getLeft(), parsedTables);
	        trav(n.getRight(), parsedTables);

	        operation = n.getOptName();
	        //System.out.println(operation);
	 
	        switch (operation) {
	            case "FROM":
	               
	                ArrayList <Object> list =(ArrayList <Object>)n.getValue();
	                BigDataScanOperator sc = null;
	                String alias=null;
	                TablesO table = new TablesO();
	                scanFilter.loadSchema(list, parsedTables);
	                this.resultSchema = scanFilter.getOutputSchema();
	                n.setSchema(this.resultSchema);
	                
	                
	                break;               
	            case "WHERE":
	                
	                list =(ArrayList <Object>)n.getValue();
	               
	                selectFilter.loadSchema(this.resultSchema);
	                this.resultSchema =  selectFilter.getOutputSchema();
	                n.setSchema(this.resultSchema);
	                
	                break;
	                
	            case "HAVING":
	                
	                list =(ArrayList <Object>)n.getValue();
	                selectFilter.loadSchema(this.resultSchema);
	                this.resultSchema =  selectFilter.getOutputSchema();
	                n.setSchema(this.resultSchema);
	                //Having hopt= new Having(o2,list);
	               	        
	                break;
	            case "GROUPBY":
	               
	            	list =(ArrayList <Object>)n.getValue();
	  
	                projFilter.loadSchema(list, resultSchema, parsedTables);
	                this.resultSchema = projFilter.getOutputSchema();
	                n.setSchema(this.resultSchema);
	               // System.out.println(this.resultSchema);
	                break;
	            case "SELECT":
	               
	                list =(ArrayList <Object>)n.getValue();
	                
	                //ExtendedProjectionOperator proj=new ExtendedProjectionOperator(o2,list);
	                projFilter.loadSchema(list, resultSchema, parsedTables);
	                this.resultSchema = projFilter.getOutputSchema();
	                n.setSchema(this.resultSchema);
	                
	              break;
	                
	            case "JOIN":
	     
	            	ArrayList<ColumnDefinition> lSchema = n.getLeft().getSchema();
	      
	            	ArrayList<ColumnDefinition> rSchema = n.getRight().getSchema();
	            	
	            	joinFilter.setChild(n.getLeft(), n.getRight());
	            	joinFilter.loadSchema(rSchema, lSchema);
	            	
	            	this.resultSchema = joinFilter.getOutputSchema();
	            	n.setSchema(this.resultSchema);
	            	
	            	
	                break;
	            case "ENHANCEDJOIN":
	            	
	            	 lSchema = n.getLeft().getSchema();
	      	      
	            	 rSchema = n.getRight().getSchema();
	            	
	            	joinFilter.loadSchema(rSchema, lSchema);
	            	this.resultSchema = joinFilter.getOutputSchema();
	            	n.setSchema(this.resultSchema);
	            	
	                break;
	            	
	            case "ORDERBY":
	            	
	            	list = (ArrayList<Object>) n.getValue();
	            	selectFilter.loadSchema(this.resultSchema);
	                this.resultSchema =  selectFilter.getOutputSchema();
	                n.setSchema(this.resultSchema);
	            	//OrderByOperator orderBy = new OrderByOperator(o2, list);
	            	
	            	break;
	            	
	            case "LIMIT":
	            	list = (ArrayList<Object>) n.getValue();
	            	selectFilter.loadSchema(this.resultSchema);
	                this.resultSchema =  selectFilter.getOutputSchema();
	                n.setSchema(this.resultSchema);
	            	//LimitOperator limitOp = new LimitOperator(o2, list);
	            	break;
	            default:
	                System.out.println("Error in tree trav");

	        }
	        
	    }

}
