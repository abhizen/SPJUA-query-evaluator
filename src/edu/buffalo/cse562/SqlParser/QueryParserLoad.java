package edu.buffalo.cse562.SqlParser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Logger;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.buffalo.cse562.Load.LoadPhase;

public class QueryParserLoad {

    Environment myDbEnvironment = null;
    Database myDatabase = null;
    File path=null;

    public static final Logger logger = Logger.getLogger(QueryParser.class.getName());

    public ArrayList<Statement> readQueries(File sql) {

        ArrayList<Statement> queries = new ArrayList<Statement>();

        try {

            FileReader fr = new FileReader(sql);
            CCJSqlParser parser = new CCJSqlParser(fr);
            Statement stmt;

            try {
                while ((stmt = parser.Statement()) != null) {
                    if ((stmt instanceof CreateTable) || (stmt instanceof Select)) {
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

    public void parseCreate(ArrayList<CreateTable> createqueries, Environment myDbEnvironment, File dataDir) {
        try {
            this.myDbEnvironment = myDbEnvironment;

            //System.out.println(createqueries);
            for (CreateTable ct : createqueries) {

                // Open the database. Create it if it does not already exist.
                DatabaseConfig dbConfig = new DatabaseConfig();
                dbConfig.setAllowCreate(true);
                dbConfig.setAllowCreate(true);
                //dbConfig.setSortedDuplicates(true);

                myDatabase = myDbEnvironment.openDatabase(null, ct.getTable().getName(), dbConfig);
                for (File data : dataDir.listFiles()) {

                    if (data.getName().endsWith(".tbl") || data.getName().endsWith(".dat")) {
                        if(ct.getTable().getName().toLowerCase().equals(data.getName().substring(0,data.getName().lastIndexOf('.')))||ct.getTable().getName().toUpperCase().equals(data.getName().substring(0,data.getName().lastIndexOf('.')))){
                            path=data;
                        }
                    }
                }
                
                
                ArrayList<ColumnDefinition> schema = (ArrayList<ColumnDefinition>) ct.getColumnDefinitions();
                //Start load phase
                LoadPhase load=new LoadPhase(myDatabase,path,myDbEnvironment, schema);
                
                if(ct.getTable().getName().equals("lineitem")||ct.getTable().getName().equals("LINEITEM")){
                    
                    myDatabase=load.processLineItem();
                }
                if(ct.getTable().getName().equals("orders")||ct.getTable().getName().equals("ORDERS")){
                    myDatabase=load.processOrders();
                }
                if(ct.getTable().getName().equals("customer")||ct.getTable().getName().equals("CUSTOMER")){
                    myDatabase=load.processCustomer();
                }
                if(ct.getTable().getName().equals("supplier")||ct.getTable().getName().equals("SUPPLIER")){
                    myDatabase=load.processSupplier();
                }
                if(ct.getTable().getName().equals("partsupp")||ct.getTable().getName().equals("PARTSUPP")){
                    myDatabase=load.processPartSupp();
                }
                if(ct.getTable().getName().equals("nation")||ct.getTable().getName().equals("NATION")){
                    myDatabase=load.processNation();
                }
                if(ct.getTable().getName().equals("region")||ct.getTable().getName().equals("REGION")){
                    myDatabase=load.processRegion();
                }
                
                
                if (myDatabase != null) {
                    myDatabase.close();
                }
            }

        } catch (DatabaseException dbe) {
            System.out.println("Error in database / environment" + dbe);
        }
    }

}
