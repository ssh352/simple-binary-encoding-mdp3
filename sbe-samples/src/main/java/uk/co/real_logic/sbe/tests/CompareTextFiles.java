package uk.co.real_logic.sbe.tests;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class CompareTextFiles
{


    private static final int BUFFERED_READER_CAPACITY = 10000 * 1024;

    public static boolean CompareTextFiles(String filename1, String filename2) throws IOException {
        BufferedReader reader1;
        BufferedReader reader2;
        System.out.println("opening file1: " + filename1);
        reader1 = new BufferedReader(new FileReader(filename1), BUFFERED_READER_CAPACITY );

        System.out.println("opening file2: " + filename2);
        reader2 = new BufferedReader(new FileReader(filename2), BUFFERED_READER_CAPACITY );


        String line1 = reader1.readLine();

        String line2 = reader2.readLine();

        boolean areEqual = true;

        int lineNum = 1;

        while (line1 != null || line2 != null) {


            byte[] line1Bytes = line1.getBytes(StandardCharsets.UTF_8);
            String line1utf = new String(line1Bytes, StandardCharsets.UTF_8);

            byte[] line2Bytes = line2.getBytes(StandardCharsets.UTF_8);
            String line2utf = new String(line2Bytes, StandardCharsets.UTF_8);

            if (line1 == null || line2 == null) {
                areEqual = false;

                break;
            } else if (!line1utf.equals(line2utf)) {

                areEqual = false;
                System.out.println("found unequal line");
                compareSplits(line1utf, line2utf);
            }

            line1 = reader1.readLine();

            line2 = reader2.readLine();

            lineNum++;
        }

        if (areEqual) {
            System.out.println("Two files have same content.");
        } else {
            System.out.println("Two files have different content. They differ at line " + lineNum + "\n");

            System.out.println("File1 has " + line1 + " and File2 has " + line2 + " at line " + lineNum);
        }

        reader1.close();

        reader2.close();
        return areEqual;
    }
    static void compareSplits(String line1, String line2) {
        List<String> line1split = Arrays.asList(line1.split(","));
        List<String> line2split = Arrays.asList(line2.split(","));
        for (int i = 0; i < line1split.size(); i++) {
            String wordFromOne = line1split.get(i);
            String wordFromTwo = line2split.get(i);
            if (!wordFromOne.equals(wordFromTwo)) {
                {
                    //    System.out.println("word :" + i);
                    System.out.println(wordFromOne);
                    System.out.println(wordFromTwo);
                    //   System.out.println("not equal");
                }

            }
        }
    }}

