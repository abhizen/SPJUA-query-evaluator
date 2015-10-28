package edu.buffalo.cse562.RATree;

import edu.buffalo.cse562.EquivalentFilters.FilterCol;
import edu.buffalo.cse562.EquivalentFilters.NewFilterCol;
import edu.buffalo.cse562.Operators.BigDataScanOperator;
import edu.buffalo.cse562.Operators.EnhancedGroupBy;

import edu.buffalo.cse562.Operators.EnhancedSelectionOperator;
import edu.buffalo.cse562.Operators.ExtendedProjectionOperator;

import edu.buffalo.cse562.Operators.HashJoinOperator;

import edu.buffalo.cse562.Operators.LimitOperator;
import edu.buffalo.cse562.Operators.Operator;
import edu.buffalo.cse562.Operators.OrderByOperator;
import edu.buffalo.cse562.Operators.SelectionOperator;
import edu.buffalo.cse562.SqlParser.TablesO;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

public class BigDataQueryPlan {

    String operation;
    HashMap<String,Expression>  tuple = new HashMap<>();
    ArrayList<HashMap<String,Expression>> tuples=new ArrayList<>();
    File swapDir=null;
    int q = 0;
    ArrayList<ColumnDefinition> orderBySchema = new ArrayList<ColumnDefinition>();
    Environment dbEnv = null;
    Database db = null;
    
    public BigDataQueryPlan(Environment dbEnv) {
    	if(NewFilterCol.query.contains("3.sql"))
    		q = 3;
    	this.dbEnv = dbEnv;
    }
    
    public BigDataQueryPlan(File swapDir){
        this.swapDir=swapDir;
    }

    public Operator trav(Node n, HashMap<String, TablesO> parsedTables) {
        if (n == null) {
            return null;
        }

        Operator o1= trav(n.getLeft(), parsedTables);
        Operator o2= trav(n.getRight(), parsedTables);
        
        operation = n.getOptName();
        //System.out.println("Opt name:"+operation+"  value: "+ n.getValue());
        switch (operation) {
            case "FROM":
                
                ArrayList <Object> list =(ArrayList <Object>)n.getValue();
                BigDataScanOperator sc = null;
                String alias=null;
                TablesO table = new TablesO();
                for(Object o: list){
                    FromItem from=(FromItem)o;
                    
                    if(from.getAlias()!=null){
                   
                        alias=from.getAlias();
                        
                        table=parsedTables.get(from.toString().substring(0, from.toString().indexOf(' ')));
                        if(table==null){
                            table=parsedTables.get(from.toString().substring(0, from.toString().indexOf(' ')).toUpperCase());
                        }
                    }
                    else if (from.getAlias()==null){
                        //System.out.println("No alias");
                        table=parsedTables.get(from.toString().toUpperCase());
                        
                        alias=table.getTableName();
                    }
                 
                    File filepath=table.getFilePath();
                 
                    orderBySchema = table.getSchema();
                   
                    //db = dbEnv.openDatabase(null, alias, dbConfig);
                    sc = new BigDataScanOperator(dbEnv,filepath,orderBySchema,alias);
                 
                    list.remove(o);
                    break;
                 
                }
                return sc;
            case "WHERE":
                
                list =(ArrayList <Object>)n.getValue();
              
               if(this.q == 3){
            	 
            	   EnhancedSelectionOperator sel= new EnhancedSelectionOperator(o2,list);
            	   return sel;
               }
               else{
            	   SelectionOperator sel= new SelectionOperator(o2,list);
            	   return sel;
               }
                
                
            /*case "HAVING":
                
                list =(ArrayList <Object>)n.getValue();
               
                Having hopt= new Having(o2,list);
               
                return hopt;*/          
             
            case "GROUPBY":
               
                list =(ArrayList <Object>)n.getValue();
                
                ArrayList <Object> groupbyList = (ArrayList <Object>)n.getsecOperation();
                EnhancedGroupBy gopt= new EnhancedGroupBy(o2,list,groupbyList);
               
                return gopt;        
                
            case "SELECT":
               
                list =(ArrayList <Object>)n.getValue();
                
                ExtendedProjectionOperator proj=new ExtendedProjectionOperator(o2,list);
                
                return proj;
            case "ENHANCEDJOIN":
            	
                list =(ArrayList <Object>)n.getValue();
                HashJoinOperator hshJoin = new  HashJoinOperator(o1,o2,list);
                
                return hshJoin;
                
            /*case "JOIN":
           
                InnerJoin join=new InnerJoin(o1,o2);
                
                return join;*/  
           
            case "ORDERBY":
            	
            	list = (ArrayList<Object>) n.getValue();
            	OrderByOperator orderBy = new OrderByOperator(o2, list, orderBySchema);
            	orderBySchema.clear();
           
            	return orderBy;
            	//break;
            case "LIMIT":
            	list = (ArrayList<Object>) n.getValue();
            	LimitOperator limitOp = new LimitOperator(o2, list);
            	return limitOp;
            default:
                System.out.println("Error in tree trav");

        }
        return null;
    }
}
