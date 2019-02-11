package com.zhuinden.sparkexperiment;
import java.util.Date;
/**
 * Created by Owner on 2017. 03. 29..
 */
public class Word {
    private String word;
    private Date data;
    private String httpcode;
    private String chamada;
    private String url;
    private long qtde;

    public String getHttpcode() {
        return httpcode;
    }

    public void setHttpcode(String httpcode) {
        this.httpcode = httpcode;
    }

    public String getChamada() {
        return chamada;
    }

    public void setChamada(String chamada) {
        this.chamada = chamada;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public long getQtde() {
        return qtde;
    }

    public void setQtde(long qtde) {
        this.qtde = qtde;
    }

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
