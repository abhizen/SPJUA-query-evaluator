package edu.buffalo.cse562;


import edu.buffalo.cse562.EquivalentFilters.NewFilterCol;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by abhinit on 9/3/15.
 */
public class EnvSetUp implements SPJUAEnvSetUp{
    private  String[]  args = null;
    private  File dataDir = null;
    private  File dbDir = null;
    private  ArrayList<File> sqlFiles = new ArrayList<File>();
    private  ArrayList<File> createFiles = new ArrayList<File>();
    private int load = 0;

    /*Parameters*/
    private static final String dataParameter = "--data";
    private static final String dbParameter = "--db";
    private static final String loadParameter = "--load";

    public EnvSetUp(String[] params){
        args = params;
        setParams();
    }

    public File getDataDir() {
        return this.dataDir;
    }

    public File getDbDir() {
        return this.dbDir;
    }


    public List<File> getSqlFiles() {
        return sqlFiles;
    }

    public List<File> getCreateFiles() {
        return createFiles;
    }

    public int getLoadParameter(){ return this.load; }

    private  void setParams(){
        int index = 0;
        String arg = null;

        while(index<args.length) {
            arg   = args[index];
            if (arg.equals(dataParameter)) {
                index++;
                dataDir = new File(args[index]);
            } else if (arg.equals(dbParameter)) {
                index++;
                dbDir = new File(args[index]);
            } else if (arg.equals(loadParameter)) {
                index++;
                createFiles.add(new File(args[index]));
                load = 1;
            } else {
                if(load!=1){
                    createFiles.add(new File(args[index]));
                    index++;
                }
                sqlFiles.add(new File(args[index]));
                NewFilterCol.setQuery(args[index]);
            }
            index++;
        }
    }
}
