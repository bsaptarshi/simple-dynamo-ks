package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by saptarshi on 3/27/15.
 */
public class Node {
    public String hashId;
    public String predPortNo;
    public String succPortNo;
    public String portNo;
    public String replicaOnePortNo;
    public String replicaTwoPortNo;

    public Node() {
        this.hashId = "";
        this.portNo = "";
        this.predPortNo = "";
        this.succPortNo = "";
        this.replicaTwoPortNo = "";
        this.replicaTwoPortNo = "";
    }
}
