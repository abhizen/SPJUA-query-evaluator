package edu.buffalo.cse562.Operators;

import java.util.ArrayList;
import java.util.HashMap;



import java.util.Iterator;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class SqlExpression {
	static HashMap<String,String> dataTypeMap = null;
	
	public static void  loadSqlExpression(ArrayList<ColumnDefinition> schema){
		if(dataTypeMap==null)
			dataTypeMap  = new HashMap<String,String>();
		
		Iterator<ColumnDefinition> itr = schema.iterator();
		ColumnDefinition colDef = null;
		String colDataType = null;
		String dataType  = null;
		
		while(itr.hasNext()){
			colDef = itr.next();	
	        if(colDef!=null)
	        	colDataType = colDef.getColDataType().getDataType();
	        if(dataTypeMap.get(colDataType)==null){
	        	 
	        	if(colDataType.contains("(")){
	             	int index = colDataType.indexOf("(");
	             	StringBuffer colType = new StringBuffer(colDataType); 
	             	dataType = colType.substring(0, index).toUpperCase();
	             	colType = null;
	             	dataTypeMap.put(colDataType, dataType);
	             }
	        	else{
	        		dataTypeMap.put(colDataType, colDataType.toUpperCase());
	        	}
	        	
	        }
		}
	}
	
    public static Expression getExpression(ColumnDefinition colDef, String expr) {
        LeafValue obj = null;
        String colDataType = null;
        if(colDef!=null){
        	colDataType = colDef.getColDataType().getDataType();//.toUpperCase();
        	colDataType = SqlExpression.dataTypeMap.get(colDataType);
        }
        
        //System.out.println(colDataType);
        
        /*if(colDataType.contains("(")){
        	int index = colDataType.indexOf("(");
        	StringBuffer colType = new StringBuffer(colDataType); 
        	colDataType = colType.substring(0, index);
        	colType = null;
        }*/
        	
        
        //System.out.println(colDef.getColDataType());
        if (colDataType.equals("INT")) {
        	//System.out.println("number :"+expr);
            obj = new LongValue(expr);
            return (Expression) obj;
        }

      //  if (colDef.getColDataType().toString().equalsIgnoreCase("varchar")||colDef.getColDataType().toString().matches("VARCHAR(.*)")||colDef.getColDataType().toString().matches("varchar(.*)")) {
        if (colDataType.equals("VARCHAR")) {
        	//String strExpr = " " + expr + " ";
        	StringBuilder strExpr = new StringBuilder(" ");
        	strExpr.append(expr);
        	strExpr.append(" ");
            
            obj = new StringValue(strExpr.toString());
            strExpr = null;
            
            return (Expression) obj;
        }
        
        if (colDataType.equals("CHAR")) {
            
        	StringBuilder strExpr = new StringBuilder(" ");
        	strExpr.append(expr);
        	strExpr.append(" ");
            
            obj = new StringValue(strExpr.toString());
            strExpr = null;
            
            return (Expression) obj;
        }

        if (colDataType.equals("STRING")) {
        	StringBuilder strExpr = new StringBuilder(" ");
        	strExpr.append(expr);
        	strExpr.append(" ");
            
            obj = new StringValue(strExpr.toString());
            strExpr = null;
            
            return (Expression) obj;
        }

        if (colDataType.equals("DECIMAL")) {
            obj = new DoubleValue(expr);
            return (Expression) obj;
        }

        if (colDataType.equals("DATE")) {
        	StringBuilder strExpr = new StringBuilder(" ");
        	strExpr.append(expr);
        	strExpr.append(" ");
          
            obj = new DateValue(strExpr.toString());

            strExpr = null;
            
            return (Expression) obj;
        }

        return null;
    }
}
