package uk.co.real_logic.sbe.read_cme_pcaps.helpers;

public class LineCounter {
    private long linesRead=0;
    //todo: consider this taking in row counter and just doing display rather than counting
    public LineCounter(boolean runShort) {
        linesRead = 0;
    }

    public void incrementLinesRead(String extraDisplay){
        this.linesRead++;
        this.displayProgress(extraDisplay);
    }


    public void incrementLinesRead(){
        this.incrementLinesRead("");
    }

    private boolean nextLineAllowed(){
        //only run through part of buffer for debugging purposes
        long numLinesShort = 500000;
        return linesRead< numLinesShort;
    }

    private void displayProgress(String extraInfo) {
        if ((this.linesRead * 1.0 / 10000 == this.linesRead / 10000)) {
            System.out.println(this.linesRead + " " + extraInfo);
        }
    }

}
