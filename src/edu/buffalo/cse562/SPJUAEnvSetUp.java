package edu.buffalo.cse562;

import java.io.File;
import java.util.List;

/**
 * Created by abhinit on 9/19/15.
 */
public interface SPJUAEnvSetUp {
    public File getDataDir();

    public File getDbDir();


    public List<File> getSqlFiles();

    public List<File> getCreateFiles();
}
