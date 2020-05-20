package uk.co.real_logic.sbe.read_cme_pcaps.helpers;

public class LineCounter {
    private long linesRead=0;

    public LineCounter(boolean runShort) {
        linesRead = 0;
    }

    public void incrementLinesRead(){
        this.linesRead++;
        this.displayProgress();
    }

    private boolean nextLineAllowed(){
        //only run through part of buffer for debugging purposes
        long numLinesShort = 500000;
        return linesRead< numLinesShort;
    }

    private void displayProgress() {
        if ((this.linesRead * 1.0 / 10000 == this.linesRead / 10000)) {
            System.out.println(this.linesRead);
        }
    }
}
