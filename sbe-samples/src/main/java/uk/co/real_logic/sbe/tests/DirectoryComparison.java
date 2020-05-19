package uk.co.real_logic.sbe.tests;

import java.io.IOException;

public class DirectoryComparison {
    public static void compareDirectories(String path1, String path2) throws IOException {
        String[] path1Files=ListFilesInDirectory.ListFiles(path1);
        for(String filename: path1Files){
            System.out.println("comparing file: " + filename);
            FileComparison.compare_files(path1 + filename, path2 + filename);
        }
    }
}
