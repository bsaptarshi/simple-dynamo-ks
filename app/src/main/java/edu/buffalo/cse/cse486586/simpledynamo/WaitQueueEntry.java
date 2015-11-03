package edu.buffalo.cse.cse486586.simpledynamo;

import android.os.Message;

/**
 * Created by saptarshi on 4/20/15.
 */
    public class WaitQueueEntry {
        public boolean status;
        public String destinationPort;
        public String value;
        public String key;
        public int msgId;


        public WaitQueueEntry(String key, String value, boolean status, String destinationPort, int msgId) {
            this.key = key;
            this.value = value;
            this.status = status;
            this.destinationPort = destinationPort;
            this.msgId = msgId;
        }
    }
