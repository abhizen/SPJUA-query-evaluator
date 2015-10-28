package edu.buffalo.cse562;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentMutableConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.statement.Statement;
import edu.buffalo.cse562.EquivalentFilters.BreakSelect;
import edu.buffalo.cse562.EquivalentFilters.EnhancedBreakSelect;
import edu.buffalo.cse562.EquivalentFilters.FilterCol;
import edu.buffalo.cse562.EquivalentFilters.NewFilterCol;
import edu.buffalo.cse562.EquivalentFilters.PushDownSelect;
import edu.buffalo.cse562.EquivalentFilters.TransformJoin;
import edu.buffalo.cse562.Operators.Operator;
import edu.buffalo.cse562.RATree.*;
import edu.buffalo.cse562.SqlParser.*;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.*;

public class Main {

    public static void main(String[] args) {

        ArrayList<File> sqlFiles = null;
        ArrayList<File> createFiles = null;
        HashMap<String, File> fileMap = new HashMap<String, File>();
        SelectBody sb = null;
        HashMap<String, HashMap<String, ArrayList<Object>>> selectMap = null;
        HashMap<String, HashMap<String, Object>> queryMap = new HashMap<String, HashMap<String, Object>>();
        SchemaCalculator sCalc = new SchemaCalculator();

        // QueryPlan qPlan=null;
        BigDataQueryPlan bigQPlan = null;
        RATree raTree = null;
        ArrayList<RATree> treeList = null;
        ArrayList<Statement> stmtList = null;
        QueryParser qp = new QueryParser();
        QueryParserLoad qpLoad = new QueryParserLoad();
        //long start = System.nanoTime();
        File dataDir = null;
        File dbDir = null;
        Operator o = null;
        int load = 0;
        Environment myDbEnvironment = null;

        EnvSetUp env = new EnvSetUp(args);
        dataDir = env.getDataDir();
        dbDir =  env.getDbDir();
        createFiles = (ArrayList<File>)env.getCreateFiles();
        sqlFiles = (ArrayList<File>)env.getSqlFiles();
        load = env.getLoadParameter();


        if (load == 1) {

            stmtList = (ArrayList<Statement>)CreateQueryLoader.getCreateQueries(createFiles);
            LoadDatabase loadDatabase = new LoadDatabase(dbDir,dataDir,stmtList);
            loadDatabase.load();


        } else {

            DMLEnvSetUp dmlEnvSetUp = new DMLEnvSetUp();
            EnvironmentConfig envConfig = dmlEnvSetUp.getEnvConfig();
           
            myDbEnvironment = new Environment(dbDir, envConfig);
            EnvironmentMutableConfig envMutableConfig = myDbEnvironment.getMutableConfig();
            envMutableConfig.setCachePercent(90);
            myDbEnvironment.setMutableConfig(envMutableConfig);

            // Contains Table name, schema(@Overridden to string) pair
            HashMap<String, TablesO> parsedTables = new HashMap<>();
            bigQPlan = new BigDataQueryPlan(myDbEnvironment);

            stmtList = (ArrayList<Statement>)SqlFileReader.getQueryList(sqlFiles,stmtList);
            stmtList = (ArrayList<Statement>)SqlFileReader.getQueryList(createFiles,stmtList);

            Main obj1 = new Main();
            treeList = new ArrayList<RATree>();
            for (Statement stmt : stmtList) {
                if (stmt instanceof Select) {

                    sb = ((Select) stmt).getSelectBody();
                    if (!(sb instanceof Union)) {
                        selectMap = qp.parseSelect("S0", sb);
                        obj1.printMap(selectMap);
                        LoadOperators obj = new LoadOperators();
                        raTree = obj.load(selectMap);
                        treeList.add(raTree);

                    } else if (sb instanceof Union) {
                        ArrayList<PlainSelect> psList = (ArrayList<PlainSelect>) (((Union) sb)
                                .getPlainSelects());
                        if (psList.size() < 2) {

                        } else {

                            for (PlainSelect ps : psList) {
                                selectMap = qp.parseSelect("S0",
                                        (SelectBody) ps);
                                obj1.printMap(selectMap);
                                LoadOperators obj = new LoadOperators();
                                raTree = obj.load(selectMap);
                                treeList.add(raTree);
                            }
                        }
                    }
                } else if (stmt instanceof CreateTable) {
                    ArrayList<CreateTable> ctList = new ArrayList<CreateTable>();
                    ctList.add((CreateTable) stmt);
                    queryMap.putAll(qp.parseCreate(ctList));
                }
            }

            String key = null;
            HashMap<String, TablesO> newTable = new HashMap<>();
            for (Map.Entry pairs : queryMap.entrySet()) {

                key = (String) pairs.getKey();
                // Separate parsed table objects from map
                if (key.matches("CREATE(.*)")) {
                    newTable = (HashMap<String, TablesO>) pairs.getValue();
                    parsedTables.putAll(newTable);
                }

            }


            for (RATree tree : treeList) {

                for (int i = 0; i < 6; i++) {
                    sCalc.trav(tree.getRoot(), parsedTables);
                    tree.trav(tree.getRoot(), new EnhancedBreakSelect());
                    sCalc.trav(tree.getRoot(), parsedTables);

                    tree.trav(tree.getRoot(), new PushDownSelect());

                    tree.trav(tree.getRoot(), new PushDownSelect());

                    tree.trav(tree.getRoot(), new PushDownSelect());

                    tree.trav(tree.getRoot(), new PushDownSelect());

                }
                sCalc.trav(tree.getRoot(), parsedTables);
                tree.trav(tree.getRoot(), new TransformJoin());
                sCalc.trav(tree.getRoot(), parsedTables);
                tree.trav(tree.getRoot(), new PushDownSelect());

                o = bigQPlan.trav(tree.getRoot(), parsedTables);

                Expression[] tuple = null;
                HashMap<String, Integer> outputSchema = new HashMap<String, Integer>();
                String flush = "";
                String out = "";
                Expression echeck = null;
                int flag = 0;
                String strEcheck = null;
                StringBuilder strBuffFlush = new StringBuilder();

                while ((tuple = o.getTuple()) != null) {
                    flag = 1;
                   
                    outputSchema = o.getOutputSchema();

                    for (Expression ex : tuple) {

                        strEcheck = ex.toString();

                        if (strEcheck != null) {
                            ;
                            if (strEcheck.equals("0")
                                    || strEcheck.equals("0.0")
                                    || strEcheck.equals("")) {
                                flag = 1;
                            } else {
                                flag = 0;
                                break;
                            }
                        } else {
                            flag = 1;
                        }

                    }

                    if (flag == 1) {

                        continue;
                    }

                    int schemaIndex = 0;

                    int size = outputSchema.size();
                    for (int i = 0; i < size; i++) {

                        out = tuple[i].toString();

                        if (out.startsWith("'")) {
                            out = out.substring(1, out.lastIndexOf("'"));
                        }

                        strBuffFlush.append(out);
                        strBuffFlush.append("|");
                    }
                    if (tuple != null && flag == 0) {

                        System.out.println(strBuffFlush.substring(0,
                                strBuffFlush.lastIndexOf("|")));
                    }

                    strBuffFlush = null;
                    strBuffFlush = new StringBuilder();
                }
            }
            long end = System.nanoTime();
        }
        
        myDbEnvironment.close();
    }

    public void printMap(HashMap<String, HashMap<String, ArrayList<Object>>> map) {
        HashMap<String, ArrayList<Object>> subMap = null;

        for (String stmt : map.keySet()) {
            subMap = map.get(stmt);
            for (String key : subMap.keySet()) {

            }
        }
    }

}
