package uk.co.real_logic.sbe.tests;


//import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class FileComparison {
    public static void main(final String[] args) throws Exception {

        compare_files();
    }

    public static void compare_files() throws IOException {
//        File reference_file = new File("c:/marketdata/testdata/testingoutputs/earliestworkingreference.txt");
        String reference_file="c:/marketdata/testdata/testingoutputs/latestreferencefile.txt";
        //  File file2 = new File("c:/marketdata/testdata/testingoutputs/earliestworkingmerge.txt");
        String latest_output = "C:/marketdata/testdata/testingoutputs/latest.txt";
        boolean isTwoEqual = CompareTextFiles.CompareTextFiles(reference_file, latest_output);
        if (isTwoEqual) {
            System.out.println("files equal");
        } else {
            System.out.println("files NOT equal!!!!!");
        }
    }
}
