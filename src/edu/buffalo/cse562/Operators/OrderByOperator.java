package edu.buffalo.cse562.Operators;

import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

import edu.buffalo.cse562.Eval;
//import edu.buffalo.cse562.Eval.Type;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class OrderByOperator extends Eval implements Operator {

    Operator opt = null;
    ArrayList<Object> condition = null;
    /*ArrayList<ColumnDefinition> inputSchema = null;
    ArrayList<ColumnDefinition> outputSchema = null;
    HashMap<String, Expression> tuple = null;
    TreeSet<HashMap<String, Expression>> allTuples = null;*/
    HashMap<String, Integer> inputSchema = null;
    HashMap<String, Integer> outputSchema = null;
    ArrayList<ColumnDefinition> orderBySchema = null;
    Expression[] tuple = null;
    TreeSet<Expression[]> allTuples = null;
    //ArrayList<HashMap<String, Expression>> sortedTuples = null;
    ArrayList<String[]> orderCondition = null;
    //Iterator<HashMap<String, Expression>> allTuplesIterator = null;
    Iterator<Expression[]> allTuplesIterator = null;
    int tupleIndex;

    public OrderByOperator(Operator o, ArrayList<Object> condition, ArrayList<ColumnDefinition> orderSchema) {
        this.opt = o;
        this.condition = condition;
        //this.tuple = new HashMap<String, Expression>();
        this.inputSchema = opt.getOutputSchema();
        this.outputSchema = this.inputSchema;
        this.orderCondition = new ArrayList<String[]>();
        this.orderBySchema = new ArrayList<ColumnDefinition>();
        //this.sortedTuples = new ArrayList<HashMap<String, Expression>>();
        this.tupleIndex = 0;
        //System.out.print("Inside order by");
        //System.out.println(condition);
        
        for (ColumnDefinition cd : orderSchema) {
        	String cName = cd.getColumnName();
        	if (this.inputSchema.containsKey(cName)) {
        		orderBySchema.add(cd);
        	}
        }
        
        processOrderBy();
        getAllTuples();
        //sortAllTuples();
    }

    public void processOrderBy() {

        /*HashMap<String, Integer> cols = new HashMap<String, Integer>();
        for (int i = 0; i < this.inputSchema.size(); i++) {
            String name = this.inputSchema.get(i).getColumnName();
            cols.put(name, i);
        }*/
        for (Object o : this.condition) {
            String colName = o.toString();
            Boolean order = true;
            if (colName.endsWith(" DESC")) {
            	order = false;
            	colName = colName.substring(0, colName.length()-5);
            }
            String[] str = new String[3];
            //System.out.println("order by column ----> " + colName);
            if (this.inputSchema.containsKey(colName)) {
                int idx = this.inputSchema.get(colName);
                ColumnDefinition cd = new ColumnDefinition();
                if (idx < this.orderBySchema.size()) {
                	cd = this.orderBySchema.get(idx);
                	str[0] = cd.getColumnName();
                    str[1] = order.toString();
                    str[2] = cd.getColDataType().getDataType();
                } else {
                	str[0] = colName;
                	str[1] = order.toString();
                	str[2] = null;
                }
                orderCondition.add(str);
            } else {
                //System.out.println("Invalid column parameter passed to ORDER BY: " + colName);
            }
        }
        this.allTuples = new TreeSet<Expression[]>(new OrderByComparator(orderCondition, this.inputSchema));
        
    }

    public void getAllTuples() {
        //HashMap<String, Expression> currTuple = new HashMap<String, Expression>();
    	Expression[] currTuple;
        while ((currTuple = opt.getTuple()) != null) {
            /*HashMap<String, Expression> thisTuple = new HashMap<>();
            for (String s : currTuple.keySet()) {
            	thisTuple.put(s, currTuple.get(s));
            }
            allTuples.add(thisTuple);*/
        	allTuples.add(currTuple);
        }
        this.allTuplesIterator = allTuples.iterator();
        /*System.out.println("----------- # allTuples = " + allTuples.size() + " -----------");
        Iterator<HashMap<String, Expression>> itr = allTuples.iterator();
        while (itr.hasNext()) {
        	//System.out.println("" + itr.next());
        }
        //System.out.println("");*/
    }

    @Override
    //public HashMap<String, Expression> getTuple() {
    public Expression[] getTuple() {
    	if (allTuplesIterator.hasNext()) {
    		return allTuplesIterator.next();
    	}
    	return null;
    }

    //public ArrayList<ColumnDefinition> getOutputSchema() {
    public HashMap<String, Integer> getOutputSchema() {
        return outputSchema;
    }

    @Override
    public void reset() {
        opt.reset();
    }

    @Override
    public LeafValue eval(Column c) throws SQLException {
        String col = c.getColumnName();
        int colIndex = this.inputSchema.get(col);
        Expression val = this.tuple[colIndex];
        return (LeafValue) val;
    }

}
