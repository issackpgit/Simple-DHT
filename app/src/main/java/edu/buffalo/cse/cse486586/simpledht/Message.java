package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;

/**
 * Created by issackoshypanicker on 4/5/18.
 */

public class Message {

    private String type;
    private String myPort;
    private String pred;
    private String succ;
    private ContentValues values;
    private String key;
    private String output;
    private String sendPort;
    private String outStar;

    public Message() {
        this.pred = "";
        this.succ = "";
        this.key = "";
        this.values = null;
        this.sendPort = "";
        this.outStar ="";
    }

    public Message(String type, String port) {
        this.type = type;
        this.myPort = port;
        this.succ = "";
        this.pred = "";
        this.key = "";
        this.values = null;
        this.sendPort = "";
        this.outStar = "";
    }

    public ContentValues getValues() {
        return values;
    }

    public void setValues(ContentValues values) {
        this.values = values;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getPort() {
        return myPort;
    }

    public void setPort(String port) {
        this.myPort = port;
    }

    public String getPred() {
        return pred;
    }

    public void setPred(String pred) {
        this.pred = pred;
    }

    public String getSucc() {
        return succ;
    }

    public void setSucc(String succ) {
        this.succ = succ;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public String getSendPort() {
        return sendPort;
    }

    public void setSendPort(String sendPort) {
        this.sendPort = sendPort;
    }

    public String getOutStar() {
        return outStar;
    }

    public void setOutStar(String outStar) {
        this.outStar = outStar;
    }

    @Override
    public String toString() {
        if(this.values!=null)
            return type + ':' + myPort + ':' + succ + ':' + pred + ":" + (String) values.get("key") +":" + (String) values.get("value") + ":" + key +":" +output + ":"+sendPort + ":" +outStar;
        else
            return type + ':' + myPort + ':' + succ + ':' + pred + ":" + " " +":" + " " + ":" +key +":" +output + ":" + sendPort+ ":" +outStar;

    }

    public String toString1() {
        return "Type="+ type + ": MyPort =" + myPort + ": Pred =" + pred + ": Succ= " + succ + ":Key =" + key+":Output =" +output + ":SendPort ="+sendPort +":OutStar ="+outStar;
    }
}
