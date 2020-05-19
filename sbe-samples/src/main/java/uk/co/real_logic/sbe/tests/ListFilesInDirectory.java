package uk.co.real_logic.sbe.tests;

import java.io.File;

public class ListFilesInDirectory {
    public static String[] ListFiles(String path1) {
        // Creates an array in which we will store the names of files and directories
        String[] pathnames;

        // Creates a new File instance by converting the given pathname string
        // into an abstract pathname
        File f = new File(path1);

        // Populates the array with names of files and directories
        pathnames = f.list();

        // For each pathname in the pathnames array
        for (String pathname : pathnames) {
            // Print the names of files and directories
            System.out.println(pathname);
        }
        return pathnames;
    }
}
