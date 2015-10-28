package edu.buffalo.cse562.Operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse562.Eval;

import java.util.Map;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class Sum extends Eval {

    HashMap<String, Integer> inputSchema = null;
    HashMap<String, Integer> outputSchema = null;
    Expression[] tuple = null;
    ArrayList<Expression[]> tupleList = null;
    Expression exp = null;
    Operator opt = null;
    int index = 0;
    char flag = 0;

    public Sum(ArrayList<Expression[]> tupleList, Expression exp, char flag, HashMap<String, Integer> inputSchema) {

        this.exp = exp;
        this.tupleList = tupleList;
        this.flag = flag;
        this.inputSchema = inputSchema;
        //System.out.println("Sum" + inputSchema);
        //System.out.println("Sum" + tupleList);
    }

    public Expression getTuple() {
        ExpressionList lst = ((Function) exp).getParameters();
        List<Expression> expList = (List<Expression>) lst.getExpressions();
        LeafValue lf = null;
        double val = 0;
        double sum = 0;
        DoubleValue dValue = null;

        Iterator<Expression[]> itr = tupleList.iterator();

        while (itr.hasNext()) {
            tuple = itr.next();
            
            
            for (Expression e : expList) {

                String match = e.toString();
                if (match.contains("+")||match.contains("-")||match.contains("*")||match.contains("/")) {
                    try {
                        lf = this.eval(e);
                        val=lf.toDouble();
                        sum=sum+val;
                        
                        //System.out.println(lf);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }

                } else {

                    for (Map.Entry entry : inputSchema.entrySet()) {
                        String schemaName = entry.getKey().toString();

                        //System.out.println(schemaName);
                        if (schemaName.matches("(.*)" + match)) {

                            match = entry.getKey().toString();
                        }
                    }

                    //System.out.println(match);
                    lf = (LeafValue) tuple[inputSchema.get(match)];
                    if (lf != null) {
                        try {
                            val = lf.toDouble();
                            //System.out.println(val);
                            sum = sum + val;
                        } catch (InvalidLeaf e1) {
                            // TODO Auto-generated catch block
                            e1.printStackTrace();
                        }
                    }
                }
            }

            index++;
        }

        if (flag == 'Y') {
            sum = sum / index;
        }

        dValue = new DoubleValue(sum);
        //tuple = new HashMap<String, Expression>();
        //tuple.put(this.exp.toString(), dValue);
        return dValue;
    }

   
    public LeafValue eval1(Column c) throws SQLException {

        String col = c.getColumnName();
        for (Map.Entry entry : inputSchema.entrySet()) {
            String schemaName = entry.getKey().toString();

            //System.out.println(schemaName);
            if (schemaName.matches("(.*)" + col)) {

                col = entry.getKey().toString();
            }
        }
        System.out.println(col);
        Expression val = this.tuple[inputSchema.get(col)];
        //System.out.println(col); 
        //System.out.println(val);

        return (LeafValue) val;
    }
    
    public LeafValue eval(Column c) throws SQLException {
        
        Table t = c.getTable();
    
        String col = c.getColumnName().toUpperCase();
        //String col1 = c.getColumnName();
        StringBuilder colName = null;
         
        Expression val = null;
        if(inputSchema.get(col)!= null){
        	val = this.tuple[inputSchema.get(col)];
        	 return (LeafValue) val;
        }
        else{
            if (t.getName() != null) {
            	colName = new StringBuilder(t.getName());
            	colName.append(".");
            	colName.append(col);
            	
            	if(inputSchema.get(colName.toString())!=null){
            		val = this.tuple[inputSchema.get(colName.toString())];
            		colName = null;
                	return (LeafValue) val;
            	}
                else {
                		colName = null;
                    	colName = new StringBuilder(t.getName().toUpperCase());
                    	colName.append(".");
                    	colName.append(col);
                    	
                    	if(inputSchema.get(colName.toString())!=null){
                    		val = this.tuple[inputSchema.get(colName.toString())];
                    		colName = null;
                        	return (LeafValue) val;
                    	}
                    	else{
                    		System.out.print("SELCTION :: val is null");
                            System.out.print("SELECTION : value of column:" + col);
                            System.out.println("SELECTION :: value of tuple" + tuple);
                    		return null;
                    	}
                }
            }
        }

        return null;
    }
}
