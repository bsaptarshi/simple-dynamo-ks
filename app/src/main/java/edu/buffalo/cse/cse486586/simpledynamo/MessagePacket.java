package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

/**
 * Created by saptarshi on 3/27/15.
 */
public class MessagePacket {
    public String message;
    public String msgType;
    public String senderPort;
    public String additionalInfo;
    public String fromPort;
    public String msgId;

    public MessagePacket(String message, String msgType, String senderPort, String additionalInfo, String fromPort, int msgId) {
        this.message = message;
        this.msgType = msgType;
        this.senderPort = senderPort;
        this.additionalInfo = additionalInfo;
        this.fromPort = fromPort;
        this.msgId = Integer.toString(msgId);
    }
}


