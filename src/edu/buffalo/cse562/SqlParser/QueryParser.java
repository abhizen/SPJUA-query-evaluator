package edu.buffalo.cse562.SqlParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
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


public class QueryParser {
	
	public static final Logger logger = Logger.getLogger(QueryParser.class.getName());
	
	public  ArrayList<Statement> readQueries (File sql) {
		
		ArrayList<Statement> queries = new ArrayList<Statement>();
		
    	try {
    		
			FileReader fr = new FileReader(sql);
			CCJSqlParser parser = new CCJSqlParser(fr);
			Statement stmt;
			
			try {
				while ((stmt = parser.Statement()) != null) {
					if ((stmt instanceof CreateTable) || (stmt instanceof Select)){
						queries.add(stmt);
					} else {
						logger.warning("PANIC! I don't know how to handle: ");
						logger.warning("" + stmt);
					}
				}
			} catch (ParseException e) {
				logger.severe("PARSE EXCEPTION IN READING FILE: " + sql);
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			logger.severe("SQL FILE NOT FOUND: " + sql);
			e.printStackTrace();
		}
		
    	return queries;
	}
	
	
	public   HashMap<String, HashMap<String, Object>> parseCreate(ArrayList<CreateTable> createqueries) {
		  HashMap<String, HashMap<String, Object>> queryMap = new HashMap<String, HashMap<String, Object>>();
          HashMap<String, Object> tables = new HashMap<String, Object>();
	
          
          String key=null;
          for(CreateTable ct : createqueries){
              
          
              TablesO newTableO = new TablesO(ct.getTable().getName(),ct.getColumnDefinitions().size());
              //newTableO.colDef = (ArrayList<ColumnDefinition>)ct.getColumnDefinitions();
              newTableO.setColumnDef((ArrayList<ColumnDefinition>)ct.getColumnDefinitions());
              newTableO.setSchema();                                  
              //System.out.println("\n"+ newTableO.toString());
              key="CREATE"+ct.getTable().getName();                                
              tables.put(ct.getTable().getName(), newTableO);
              
              queryMap.put(key, tables);
          }
           
          return queryMap;
	}
	
	public HashMap<String,HashMap<String, ArrayList<Object>>> parseSelect(String stmtName, SelectBody q) {
		 HashMap<String, ArrayList<Object>> queryMap = new HashMap<String, ArrayList<Object>>();
		 HashMap<String,HashMap<String, ArrayList<Object>>> tableQueryMap = new HashMap<String,HashMap<String, ArrayList<Object>>>();
		 String subQueryAlias = null;
		 int index = 1;
		 
			if (!(q instanceof Union)) {
				boolean nestedFromQuery = false;
				boolean nestedJoinQuery = false;
				PlainSelect ps = (PlainSelect)q;
				ArrayList<Object> where = new ArrayList<Object>();
				ArrayList<Object> groupBy = new ArrayList<Object>();
				ArrayList<Object> from  = new ArrayList<Object>();
				ArrayList<Object> join  = new ArrayList<Object>();
				Limit limit = new Limit();
				ArrayList<Object> orderBy = new ArrayList<Object>();
				Distinct distinct = new Distinct();
                
				String subTableAlias = null;
				SelectBody subBody = null;
				// FROM
				if (ps.getFromItem() != null) {
					
                                        if(ps.getFromItem().toString().contains("SELECT")||ps.getFromItem().toString().contains("select")){
                                           
                                            FromItem fromItem=ps.getFromItem();
                                            subBody=((SubSelect)fromItem).getSelectBody();
                                            subTableAlias=((SubSelect)fromItem).getAlias();
                                            
                                            if(subTableAlias==null){
                                            	subTableAlias = stmtName.substring(0, stmtName.length()-2) + String.valueOf(index);
                                            	index++;
                                            	fromItem.setAlias(subTableAlias);
                                            }
                
                                            tableQueryMap.putAll(parseSelect(subTableAlias,subBody));   
                                           // ArrayList<Object> from  = new ArrayList<Object>();
                                            from.add(fromItem);
                                            queryMap.put("FROM",from);
                                            //System.out.println("querymap: "+queryMap);
                                            //queryMap.
                                        }
                                        else{
                                        	Object fromItem  = ps.getFromItem();
                                        	//ArrayList<Object> from  = new ArrayList<Object>();
                                        	 from.add(fromItem);
                                        	queryMap.put("FROM", from);
                                        	//System.out.println("querymap: "+queryMap);
                                        }
				}
				
				// JOIN
				if (ps.getJoins() != null) {
					
					/*if(ps.getJoins().toString().contains("SELECT")||ps.getFromItem().toString().contains("select")){
                        //System.out.println("-------------------------");
                        //System.out.println("Detected subquery");
                        //System.out.println("--------------------------");*/
                        for(Object ob : ps.getJoins()){
                        	Join j = (Join)ob;
                        	FromItem newfromItem = (FromItem)j.getRightItem();
                        	
           
                        	if(newfromItem instanceof SubSelect){
                        		SubSelect newsubBody=(SubSelect)newfromItem;
                        		subTableAlias=((SubSelect)newfromItem).getAlias();
                        		
                        		if(subTableAlias==null){
                                 	subTableAlias = stmtName.substring(0, stmtName.length()-1) + String.valueOf(index);
                                 	index++;
                                 	newfromItem.setAlias(subTableAlias);
                                 }
                        		
                        		tableQueryMap.putAll(parseSelect(subTableAlias, newsubBody.getSelectBody()));   
                                //ArrayList<Object> from  = new ArrayList<Object>();
                                from.add(newfromItem);
                                queryMap.put("FROM",from);
                                //ArrayList<Object> join  = new ArrayList<Object>();
                                join.add(index);
                                 queryMap.put("JOIN",join);
                                 //System.out.println("querymap: "+queryMap);
                        	}
                        	else{
                        		//ArrayList<Object> from  = new ArrayList<Object>();
                                from.add(newfromItem);
                                queryMap.put("FROM",from);
                                join.add(index);
        						queryMap.put("JOIN", join);
        						//System.out.println("querymap: "+queryMap);
                        	}
                        }
                   // }	
					/*else{
						ArrayList<Object> join = (ArrayList<Object>)ps.getJoins();
						queryMap.put("JOIN", join);
					}*/
				}
				
				// WHERE
				if (ps.getWhere() != null) {
					Object whereObj = ps.getWhere();
					ArrayList<Object> whereList = new ArrayList<Object>();
					whereList.add(whereObj);
					queryMap.put("WHERE", whereList);
					
				}
				
				// SELECT
				if (ps.getSelectItems() != null) {
					ArrayList<Object> select = (ArrayList<Object>)ps.getSelectItems();
					queryMap.put("SELECT", select);
					
				}
				// GROUP BY
				if (ps.getGroupByColumnReferences() != null) {
					groupBy = (ArrayList<Object>)ps.getGroupByColumnReferences();
					//groupBy = (ArrayList<Object>)ps.getSelectItems();
					queryMap.put("GROUPBY", groupBy);
				}
				
				// HAVING
				if (ps.getHaving() != null) {
					Object having = ps.getHaving();
					ArrayList<Object> havingList = new ArrayList<Object>();
					havingList.add(having);
					queryMap.put("HAVING", havingList);
					
				}
				
				// LIMIT
				if (ps.getLimit() != null) {
					limit = ps.getLimit();
					ArrayList<Object> limitList = new ArrayList<Object>();
					limitList.add((Object)limit);
					queryMap.put("LIMIT", limitList);
					
				}
				
				// ORDER BY
				if (ps.getOrderByElements() != null) {
					orderBy = (ArrayList<Object>)ps.getOrderByElements();
					queryMap.put("ORDERBY", orderBy);
					
				}
				
				// DISTINCT
				if (ps.getDistinct() != null) {
					distinct = ps.getDistinct();
					ArrayList<Object> distinctList = new ArrayList<Object>();
					distinctList.add((Object)distinct);
					queryMap.put("DISTINCT", distinctList);
					
				}
           }
		tableQueryMap.put(stmtName, queryMap);
		return tableQueryMap;
	}
	
}