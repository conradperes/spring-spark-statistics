package com.zhuinden.sparkexperiment;
import java.util.Date;
/**
 * Created by Owner on 2017. 03. 29..
 */
public class Word {
    private String word;
    private Date data;

    public Word() {
    }

    public Word(Date data) {
        this.data = data;
    }

    public Date getData() {
        return data;
    }

    public void setData(Date data) {
        this.data = data;
    }

    public Word(String word) {
        this.word = word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public String getWord() {
        return word;
    }
}
