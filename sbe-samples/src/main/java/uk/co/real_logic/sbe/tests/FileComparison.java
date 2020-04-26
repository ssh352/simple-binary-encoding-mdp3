package uk.co.real_logic.sbe.tests;


import org.apache.commons.io.FileUtils;

import java.io.File;

public class FileComparison {
    public static void main(final String[] args) throws Exception {

        File reference_file = new File("c:/marketdata/testdata/testingoutputs/earliestworkingreference.txt");
        //  File file2 = new File("c:/marketdata/testdata/testingoutputs/earliestworkingmerge.txt");
        File latest_output = new File("c:/marketdata/testdata/testingoutputs/latest.txt");
        boolean isTwoEqual = FileUtils.contentEquals(reference_file, latest_output);
        if (isTwoEqual) {
            System.out.println("files equal");
        } else {
            System.out.println("files NOT equal!!!!!");
        }
    }
}