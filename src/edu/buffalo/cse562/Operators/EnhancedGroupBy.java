package edu.buffalo.cse562.Operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

public class EnhancedGroupBy  implements Operator {

    HashMap<String,Integer> inputSchema = null;
    HashMap<String,Integer> outputSchema = null;
    Expression[] tuple = null;
     Expression[] tupleTemp = null;
    HashMap<String,String> inputschemaMap = new HashMap<String,String>();
    
    ArrayList<Object> selectItmList = null;
    ArrayList<Object> groupbyItmList = null;
    Operator opt = null;
    ArrayList<HashMap<String, Expression>> tupleList = null;
    int index = 0;
    private int sortFlag = 0;
    HashMap<String, EnhancedSumGroupBy> lst = null;
    Iterator lstItr = null;

    public EnhancedGroupBy(Operator opt, ArrayList<Object> selectItmList, ArrayList<Object> groupbyItmList) {
        this.opt = opt;
        this.selectItmList = selectItmList;
        this.groupbyItmList = groupbyItmList;
        this.tupleList = new ArrayList<HashMap<String, Expression>>();

        this.inputSchema = opt.getOutputSchema();
        this.outputSchema = new HashMap<String,Integer>();
        this.loadSchemaMap();
        
        this.process();
        lstItr = lst.keySet().iterator();
    }
    
    public void setSortFlag(){
    	this.sortFlag = 1;
    }
    
    public void resetSortFlag(){
    	this.sortFlag = 0;
    }
    
    public int getsortFlag(){ return this.sortFlag;}

    private void process() {
        Expression expr = null;
        SelectExpressionItem sExp = null;
      
       
        
      
        Iterator itr = this.groupbyItmList.iterator();
      
        int i = 0;
       
        itr = this.selectItmList.iterator();
      
        while (itr.hasNext()) {
            sExp = (SelectExpressionItem) itr.next();
            loadOutputSchema(sExp,i);
            i++;
        }
        lst = this.GroupByArrayList();
    }


    private void loadOutputSchema(SelectExpressionItem e, int i) {

        String alias = null;
        alias = ((SelectExpressionItem) e).getAlias();
        String colName = null;
       
        Expression a = (Expression) e.getExpression();

        if (a instanceof Function) {
            if (alias != null) {
               // c1.setColumnName(alias);
            	colName = alias;
            } else {
                //c1.setColumnName(a.toString());
            	colName = a.toString();
            }
        } else {
            if (alias != null) {
                //c1.setColumnName(alias);
            	colName = alias;
            } else {
            	colName = a.toString();
            }
        }
        this.outputSchema.put(colName,new Integer(i));
       
    }

    @Override
    public HashMap<String, Integer> getOutputSchema() {
        //System.out.println(outputSchema);
        return this.outputSchema;

    }

    @Override
    public Expression[] getTuple() {
       
    	String aggKey = null;
    	Map.Entry<String,EnhancedSumGroupBy> obj = null;
    	EnhancedSumGroupBy tempObj = null;
    	 Expression[] tuple = null;
    	if(lstItr.hasNext()){
    		aggKey =  (String) lstItr.next();
    		tempObj = this.lst.get(aggKey);
    		tuple = tempObj.getTuple().clone();
    		
    		return tuple;
    	}
    	else
    		return null;
    }

    @Override
    public void reset() {
        opt.reset();
    }
    public HashMap<String, EnhancedSumGroupBy>
    	GroupByArrayList() {
   	
   	Iterator colomnListItr = null;
   	Column col = null;
   	ArrayList<Expression[]> listExpr = new ArrayList<Expression[]>();
   	HashMap<String, EnhancedSumGroupBy> list = new
   			HashMap<String, EnhancedSumGroupBy>();
   	ArrayList<Expression[]> tempList = null;
   	Expression e1 = null;
   	StringBuilder key = null;
   	EnhancedSumGroupBy obj = null;
   	int count = 1;
   	
   	while ((tuple = opt.getTuple())!=null) {
   		colomnListItr = this.groupbyItmList.iterator();
   		key = null;
   		while(colomnListItr.hasNext()){
   			col = (Column) colomnListItr.next();
   			try {
   				e1 = (Expression) this.eval(col);
   				if(key==null && e1!=null)
   					key = new StringBuilder(e1.toString());
   				else{
   					if(e1!=null)
   						key.append(e1.toString());//key.concat(e1.toString());
   				}
   			}
   			catch (SQLException e) {
   				e.printStackTrace();
   			}
   		}
   			if (list!=null && (obj = list.get(key.toString())) != null) {
   				tempList = new ArrayList<Expression[]>();
   				tempList.add(tuple);
   				obj.setTupleList(tempList);
   				obj.calcTuple();
   				list.put(key.toString(), obj);
   				tempList = null;
   			}
   			else {
   				tempList = new ArrayList<Expression[]>();
   				tempList.add(tuple);
   				obj = new EnhancedSumGroupBy(tempList,  this.selectItmList, inputSchema, this.outputSchema);
   				obj.calcTuple();
   				list.put(key.toString(), obj);
   				tempList = null;
   			}
   			
   			count++;
   		
   	}

   	return list;
   }
    
      public LeafValue eval(Column c) throws SQLException {
        Table t = c.getTable();
        String col = c.getColumnName().toUpperCase();
        Expression val = null; //this.tuple.get(col);
        int index = 0;
        
        
        
        if(this.inputSchema.get(col)!=null){
        	index =  this.inputSchema.get(col);
        	val = this.tuple[index];
    	}else{
    		col = inputschemaMap.get(col);
    		index =  this.inputSchema.get(col);
    		val = this.tuple[index];
    	}
        	
         return (LeafValue) val;

    }
    
    public void loadSchemaMap(){
    	StringBuilder colName = null; 
    	String col = null;
    	int index = 0;
    	
    	for (String cd : this.inputSchema.keySet()) {
    		col = cd.toUpperCase();
    		if(col.contains(".")){
    			index = col.indexOf(".");
    			colName = new StringBuilder(col);
    			col = colName.substring(index+1, col.length());
    			colName = null;
    		}
    		if(this.inputschemaMap.get(col)==null)
    			this.inputschemaMap.put(col, cd);
    	}
    }

}
