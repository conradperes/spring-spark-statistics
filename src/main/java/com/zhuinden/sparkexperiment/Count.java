package com.zhuinden.sparkexperiment;
import java.util.Date;
/**
 * Created by Owner on 2017. 03. 29..
 */
public class Count {
    private String word;
    private long count;
    private Date data;

    public Date getData() {
        return data;
    }

    public void setData(Date data) {
        this.data = data;
    }

    public Count(String word, long count, Date data) {
        this.word = word;
        this.count = count;
        this.data = data;
    }

    public Count() {
    }

    public Count(String word, long count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
