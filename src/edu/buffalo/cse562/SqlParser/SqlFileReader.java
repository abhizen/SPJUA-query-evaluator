package edu.buffalo.cse562.SqlParser;

import net.sf.jsqlparser.statement.Statement;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by abhinit on 9/20/15.
 */
public class SqlFileReader {
    private static QueryParser qp = null;

    static {
        qp = new QueryParser();
    }

    public static List<Statement>  getQueryList(List<File> sqlFiles,List<Statement> stmtList){
        for (File sql : sqlFiles) {
            if (stmtList == null) {
                stmtList = qp.readQueries(sql);
            } else {
                stmtList.addAll(qp.readQueries(sql));
            }
        }

        return stmtList;
    }
}
