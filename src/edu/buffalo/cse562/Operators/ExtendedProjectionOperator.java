package edu.buffalo.cse562.Operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import edu.buffalo.cse562.Eval;
import java.util.Map;
//import edu.buffalo.cse562.Eval.Type;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ExtendedProjectionOperator extends Eval implements Operator {

    Operator opt = null;
    ArrayList<Object> condition = null;
    HashMap<String, Integer> inputSchema = null;
    HashMap<String, Integer> outputSchema = null;
    Expression[] tuple = null;
    String alias = null;
    Expression[] resultTuple = null;
    ArrayList<Expression[]> tupleList = new ArrayList<Expression[]>();
    int nullFlag = 0;

    public ExtendedProjectionOperator(Operator opt, ArrayList<Object> condition) {

        this.opt = opt;
        this.condition = condition;
        this.inputSchema = opt.getOutputSchema();
        this.outputSchema = new HashMap<>();//this.inputSchema;;//this.inputSchema;
        //this.resultTuple = new Expression;
        int i=0;
        
        for (Object o : condition) {
            if (o instanceof SelectItem) {
                SelectItem obj = (SelectItem) o;
                Expression e = ((SelectExpressionItem) obj).getExpression();
                String alias = null;
                alias = ((SelectExpressionItem) obj).getAlias();
                ColumnDefinition c1 = new ColumnDefinition();
                ColDataType type = new ColDataType();
                if (e instanceof Function) {
                    type.setDataType("double");
                    c1.setColDataType(type);
                    c1.setColumnName(e.toString());
                } else {
                    type.setDataType("string");
                    c1.setColDataType(type);
                    if (alias != null) {
                        c1.setColumnName(alias);
                    } else {
                        c1.setColumnName(obj.toString());
                    }
                }
                this.outputSchema.put(c1.getColumnName(), i);
                i++;
            }
        }
        
        resultTuple=new Expression[i];
        //System.out.println("EXPROJ: " + outputSchema);
    }

    public Expression[] getTuple() {
        Expression[] tempTuple = null;
        int tupleItr=0;
        if(nullFlag==0)
        	tuple = opt.getTuple();
        else
        	return null;
        
//        for(int j=0;j<tuple.length;j++){
            //System.out.println(tuple[j]);
        //}
        if (tuple == null) {
            return null;
        }

        for (Object o : condition) {
            if (o instanceof SelectItem) {
                SelectItem obj = (SelectItem) o;

                try {
                    Expression e = ((SelectExpressionItem) obj).getExpression();

                    if (e instanceof Function) {
                        
                        tupleList.add(tuple);
                        
                        while ((tempTuple = opt.getTuple()) != null) {
                            
                            tupleList.add(tempTuple);
                            
                        }
                        nullFlag = 1;
                        
                        
                        if (((Function) e).getName().toString().equalsIgnoreCase("SUM")) {
                            Sum sumObj = new Sum(tupleList, e, 'N', inputSchema);
                            resultTuple[tupleItr]=(sumObj.getTuple());
                            tupleItr++;

                        } else if (((Function) e).getName().toString().equalsIgnoreCase("AVG")) {
                            Sum sumObj = new Sum(tupleList, e, 'Y', inputSchema);
                            resultTuple[tupleItr]=(sumObj.getTuple());
                            tupleItr++;
                            //tempTuple = sumObj.getTuple();
                        } else if (((Function) e).getName().toString().equalsIgnoreCase("MAX")) {
                            MinMax maxObj = new MinMax(tupleList, e, 'N', inputSchema);
                            resultTuple[tupleItr]=(maxObj.getTuple());
                            tupleItr++;
                            //tempTuple = maxObj.getTuple();
                        } else if (((Function) e).getName().toString().equalsIgnoreCase("MIN")) {
                            MinMax maxObj = new MinMax(tupleList, e, 'Y', inputSchema);
                            resultTuple[tupleItr]=(maxObj.getTuple());
                            tupleItr++;
                            //tempTuple = maxObj.getTuple();
                        } else if (((Function) e).getName().toString().equalsIgnoreCase("COUNT")) {

                            Count countObj = new Count(tupleList, e, inputSchema);
                            resultTuple[tupleItr]=(countObj.getTuple());
                            tupleItr++;
                            //tempTuple = countObj.getTuple();
                        }
                    } else {

                        LeafValue lf = this.eval(((SelectExpressionItem) obj).getExpression());
                        //System.out.println(lf);
                        
                        String alias = null;
                        alias = ((SelectExpressionItem) obj).getAlias();
                        if (alias != null) {
                            resultTuple[tupleItr]=((Expression) lf);
                            tupleItr++;
                        } else {
                            resultTuple[tupleItr]=((Expression) lf);
                            tupleItr++;
                        }
                        //Expression exp = ((SelectExpressionItem) obj).getExpression();
                        
                    }

                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }
        //System.out.println(resultTuple);
        return resultTuple;
    }

    public void reset() {
    	nullFlag = 0;
        opt.reset();
    }

    public LeafValue eval(Column c) throws SQLException {
        Table t = c.getTable();
        //System.out.println("Table wholename, name, alias :::" + t.getWholeTableName() + t.getName() + t.getAlias());

        String col = c.getColumnName();
        Expression val = null;
        /*System.out.println("column :"+col);
         //System.out.println("tuple :"+tuple)*/;
         if(inputSchema.get(col)!=null)
        	 val = this.tuple[inputSchema.get(col)];
         
        

        if (val == null) {
            if (t.getName() != null) {
                col = t.getName() + "." + col;
            }
            val = this.tuple[inputSchema.get(col)];
        }
        
        return (LeafValue) val;
    }

    public HashMap<String, Integer> getOutputSchema() {

        return this.outputSchema;
    }
}
