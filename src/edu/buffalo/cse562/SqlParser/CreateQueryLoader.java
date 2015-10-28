package edu.buffalo.cse562.SqlParser;

import net.sf.jsqlparser.statement.Statement;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by abhinit on 9/20/15.
 */
public class CreateQueryLoader {
    private static QueryParserLoad qpLoad = new QueryParserLoad();
    private static CreateQueryLoader Instance = null;

    private CreateQueryLoader(){}

    public static CreateQueryLoader getInstance(){
        if(Instance==null)
            Instance = new CreateQueryLoader();

        return Instance;
    }

    public static List<Statement> getCreateQueries(List<File> createFiles){
        ArrayList<Statement> stmtList = null;
        for (File sql : createFiles) {
            if (stmtList == null) {
                stmtList = qpLoad.readQueries(sql);
            } else {
                stmtList.addAll(qpLoad.readQueries(sql));
            }
        }
        return stmtList;
    }
}
