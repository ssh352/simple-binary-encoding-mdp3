package uk.co.real_logic.sbe.read_cme_pcaps.helpers;

public class LineCounter {
   private long numLinesShort = 500000; //only run through part of buffer for debugging purposes
    private long linesRead=0;
   private boolean runShort;

    public LineCounter(boolean runShort) {
        this.runShort=runShort;
        linesRead = 0;
    }

    public void incrementLinesRead(){
        this.linesRead++;
        this.displayProgress();
    }

    private boolean nextLineAllowed(){
            return linesRead<numLinesShort;
    }

    private void displayProgress() {
        if ((this.linesRead * 1.0 / 10000 == this.linesRead / 10000)) {
            System.out.println(this.linesRead);
        }
    }
}
