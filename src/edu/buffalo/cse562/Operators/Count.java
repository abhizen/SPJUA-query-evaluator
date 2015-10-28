package edu.buffalo.cse562.Operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse562.Eval;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class Count {

    HashMap<String, Integer> inputSchema = null;
    HashMap<String, Integer> outputSchema = null;
    Expression[] tuple = null;
    ArrayList<Expression[]> tupleList = null;
    Expression exp = null;
    Operator opt = null;
    int index = 0;
    List<Expression> expList = null;

    public Count(ArrayList<Expression[]> tupleList, Expression exp, HashMap<String, Integer> inputSchema) {
        this.exp = exp;
        this.tupleList = tupleList;
        this.inputSchema = inputSchema;
        //System.out.println(tupleList);
        
    }

    public Expression getTuple() {
        ExpressionList lst = ((Function) exp).getParameters();
        LeafValue lf = null;
        double val = 0;
        int count = 0;
        LongValue dValue = null;

        Iterator<Expression[]> itr = tupleList.iterator();

        if (lst != null) {
            List<Expression> expList = (List<Expression>) lst.getExpressions();
            while (itr.hasNext()) {
                tuple = itr.next();
                for (Expression e : expList) {
                    lf = (LeafValue) tuple[inputSchema.get(e.toString())];
                    if (lf != null) {
                        count++;
                    }

                }
            }
        }
        else{
            while(itr.hasNext()){
                tuple=itr.next();
                count++;
            }
        }

        dValue = new LongValue(count);
        //tuple = new HashMap<String, Expression>();
        //tuple.put(this.exp.toString(), dValue);
        return dValue;
    }

}
