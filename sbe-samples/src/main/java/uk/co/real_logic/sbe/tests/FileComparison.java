package uk.co.real_logic.sbe.tests;


//import org.apache.commons.io.FileUtils;

import java.io.IOException;

public class FileComparison {
    public static void main(final String[] args) {

    //    compare_files();
    }

    public static void compare_files(String referenceFile, String latestOutput) throws IOException {
//        File reference_file = new File("c:/marketdata/testdata/testingoutputs/earliestworkingreference.txt");
        //  File file2 = new File("c:/marketdata/testdata/testingoutputs/earliestworkingmerge.txt");
        boolean isTwoEqual = CompareTextFiles.CompareTextFiles(referenceFile, latestOutput);
        if (isTwoEqual) {
            System.out.println("files equal");
        } else {
            System.out.println("files NOT equal!!!!!");
        }
    }
}
