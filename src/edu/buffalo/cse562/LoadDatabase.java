package edu.buffalo.cse562;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import edu.buffalo.cse562.SqlParser.QueryParserLoad;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by abhinit on 9/20/15.
 */
public class LoadDatabase {
private Environment myDbEnvironment = null;
    private EnvironmentConfig envConfig = null;
    private QueryParserLoad qpLoad = new QueryParserLoad();
    private File dbDir = null;
    private File dataDir = null;
    private ArrayList<Statement> stmtList = null;

    public LoadDatabase(File dbDir, File dataDir,List<Statement> stmtList){
        this.dbDir = dbDir;
        this.dataDir = dataDir;
        this.stmtList = (ArrayList<Statement>)stmtList;
        this.setEnv();
    }

    private void setEnv(){
        // Open the environment. Create it if it does not already exist.
        envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        myDbEnvironment = new Environment(dbDir, envConfig);
    }

    public void load() throws DatabaseException{
        ArrayList<CreateTable> ctList = new ArrayList<CreateTable>();
        try {
            //iterate over create queries and create databases for each
            for (Statement stmt : stmtList) {
                ctList.add((CreateTable) stmt);

            }
            qpLoad.parseCreate(ctList,myDbEnvironment,dataDir);

            if (myDbEnvironment != null) {
                myDbEnvironment.close();
            }
        } catch (DatabaseException dbe) {
            throw dbe;
        }
    }
}
