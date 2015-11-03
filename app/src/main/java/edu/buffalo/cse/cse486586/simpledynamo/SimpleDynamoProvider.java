package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.util.HashMap;
import java.util.concurrent.locks.*;
////////////////////////////////////////////////////////////////FINAL SUBMISSION PHASE 1-6//////////////////////////////////////////////////////////////////
public class SimpleDynamoProvider extends ContentProvider {
    static final String REMOTE_PORT0 = "5554";
    static final String REMOTE_PORT1 = "5556";
    static final String REMOTE_PORT2 = "5558";
    static final String REMOTE_PORT3 = "5560";
    static final String REMOTE_PORT4 = "5562";
    static final String remote_ports[] = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};

    static final String taggeneral = "taggeneral";
    static final String taginsert = "taginsert";
    static final String tagdelete = "tagdelete";

    public static Uri URI_obj;
    public static String ATTRKEY;
    public static String ATTRVALUE;

    private ArrayList<Node> nodeMap;
    private Node headNode;
    //    private HashMap<String, WaitQueueEntry> waitQueue;
    private ArrayList<WaitQueueEntry> insertQ;
    private ArrayList<WaitQueueEntry> queryQ;

    private HashMap<Integer, String> queryResultMap;
    private HashMap<Integer, Boolean> insertResultMap;
    private int insertQCounter;
    private int queryQCounter;

    private String headNodePort;

    static final int SERVER_PORT = 10000;
    public String myPort;
    public String predPort;
    public String succPort;
    public String[] myReplicaNodePortNos;
    public String myNodeHashId;

    public String singleQueryContainer;
    public String globalQueryContainer;

    public boolean waitingIsOver;

    MatrixCursor mCursorGlobal;

    static final String MSGTYPEGLOBALDEL = "MGD";
    static final String MSGTYPESINGLEDEL = "MSD";
    static final String MSGTYPELOCALDEL = "MLD";

    static final String MSGTYPEINSERTREQUEST = "MINS";
    static final String MSGTYPEINSERTACK = "MIACK";

    static final String MSGTYPEGLOBALQUERYREQUEST = "MGQR";
    static final String MSGTYPESINGLEQUERYREQUEST = "MSQR";

    static final String MSGTYPEGLOBALQUERYRESPONSE = "MGQRE";
    static final String MSGTYPESINGLEQUERYRESPONSE = "MSQRE";

    static final String SELECTIONTYPELOCAL = "\"@\"";
    static final String SELECTIONTYPEGLOBAL = "\"*\"";

    static final String MSGTYPELOSTKEYREQUEST = "MLKR";
    static final String MSGTYPELOSTKEYRESPONSE = "MLKRE";

    static final int TIMEOUTINTERVAL = 2000;
    static final int TIMEOUTINTERVALINSERT = 100; //200, 500 best- 100
    static final int TIMEOUTINTERVALQUERY = 700;  //1500, 2000 best - 700

    private Lock queryLock;
    private Lock insertLock;
    private Lock deleteLock;


    /////////////////////////////////START OF CODE FOR DELETE///////////////////////////////////////
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if (selection.compareTo(SELECTIONTYPELOCAL) == 0) {
            deleteLocalFiles();
            MessagePacket msgToSend = generateSingleDeleteRequestMessage(selection, MSGTYPELOCALDEL);
            sendSingleDeleteRequestToReplicaNodes(myReplicaNodePortNos[0], myReplicaNodePortNos[1], msgToSend);
        } else if (selection.compareTo(SELECTIONTYPEGLOBAL) == 0) {
            deleteLocalFiles();
            sendGlobalDeleteRequestToAll(selection);
        } else {
            Node destinationNode = getNodeFromKey(selection);
//                if (checkIfHeadNode(myPort)) {
//                    if ((hashKey.compareTo(genHash(predPort)) > 0) || (hashKey.compareTo(myNodeHashId) <= 0)) {
//                        getContext().deleteFile(selection);
//                    } else {
//                        sendSingleDeleteRequestToOriginalNode(selection);
//                    }
//                } else {
//                    if (hashKey.compareTo(myNodeHashId) <= 0 && hashKey.compareTo(genHash(predPort)) > 0) {
//                        getContext().deleteFile(selection);
//                    } else {
//                        sendSingleDeleteRequestToOriginalNode(selection);
//                    }
//                }
            if (destinationNode.portNo.equals(myPort)) {
                getContext().deleteFile(selection);
            } else {
                sendSingleDeleteRequestToOriginalNode(selection, destinationNode);
            }
        }




        return 0;
    }

    private void deleteLocalFiles() {
        String[] localFileList = getContext().fileList();
        for (int i=0;i<localFileList.length;i++) {
            getContext().deleteFile(localFileList[i]);
        }
    }

    private void sendSingleDeleteRequestToOriginalNode(String selection, Node destinationNode) {
        MessagePacket msgToSend = generateSingleDeleteRequestMessage(selection, MSGTYPESINGLEDEL);
        /*
        Send single key delete request to the original node the key belongs to
         */
        sendSingleDeleteRequestToNode(destinationNode.portNo, msgToSend);
         /*
        Send single key delete request to the replica nodes of the original node the key belongs to
         */
        sendSingleDeleteRequestToReplicaNodes(destinationNode.replicaOnePortNo, destinationNode.replicaTwoPortNo, msgToSend);
    }

    private void sendSingleDeleteRequestToReplicaNodes(String replicaOnePortNo, String replicaTwoPortNo, MessagePacket msgToSend) {
        sendSingleDeleteRequestToNode(replicaOnePortNo, msgToSend);
        sendSingleDeleteRequestToNode(replicaTwoPortNo, msgToSend);
    }

    private void sendSingleDeleteRequestToNode (String portNo, MessagePacket msgToSend) {
        Socket socket = null;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(String.valueOf((Integer.parseInt(portNo) * 2))));
//            socket.setSoTimeout(TIMEOUTINTERVAL);
            //Log.i(tagdelete, "Sending single delete request to "+portNo);

            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); //write the message received to the output stream
            bw.write(serializeMessage(msgToSend));
            bw.flush();   //force invoke flush to send message and clear buffer.
            socket.close(); //close the socket.
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
            //Log.i("TIMEOUT", "TIMEOUT Occured during delete");
            if (e instanceof SocketTimeoutException) {
                //handle failure for socket timeout, node probably dead
            }
        }
    }

    private MessagePacket generateSingleDeleteRequestMessage(String selection, String msgType) {
        MessagePacket msg = new MessagePacket(selection, msgType, myPort, "NULL", myPort, -1);
        return msg;
    }

    private void sendGlobalDeleteRequestToAll(String selection) {
        for (int i=0; i<nodeMap.size(); i++) {
            if (nodeMap.get(i).portNo.equals(myPort)) {
                //do nothing, my files already deleted
            } else {
                MessagePacket msgToSend = generateSingleDeleteRequestMessage(selection, MSGTYPEGLOBALDEL);
                sendSingleDeleteRequestToNode(nodeMap.get(i).portNo, msgToSend);
            }
        }
    }

    /////////////////////////////////END OF CODE FOR DELETE///////////////////////////////////////





    /////////////////////////////////START OF CODE FOR INSERT///////////////////////////////////////
    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String key = values.getAsString(ATTRKEY);
        try {
            String hashKey = genHash(key);
            Node destinationNode = getNodeFromKey(key);
            //Log.i(taginsert, "INSREQ key = "+key+", " + " value "+values.getAsString(ATTRVALUE)+"..... This message to be inserted at node "+destinationNode.portNo);
            if (destinationNode.portNo.equals(myPort)) {
                /*
                Insert the value at my node
                 */
                insertKeyAtNode(values);
                /*
                Forward the insert request to my first replica node
                 */
                forwardInsertRequestToReplicaNode(destinationNode, values);
                return uri;
            } else {
                /*
                Forward the insert request to the appropriate destination node
                 */
                forwardInsertRequestToDestinationNode(destinationNode, values);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void insertKeyAtNode(ContentValues values) {
        String filePath = getContext().getFilesDir().getAbsolutePath();
        String key = values.getAsString(ATTRKEY);
        try {
            FileWriter fw = new FileWriter(new File(filePath, key));
            fw.write(values.getAsString(ATTRVALUE));
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void forwardInsertRequestToReplicaNode(Node destinationNode, ContentValues values) {
        int msgCounter = 2;
        int currInsertQCounter = insertQCounter;
        insertQCounter = insertQCounter + 1;
        String sendToPortNo = destinationNode.replicaOnePortNo;
        MessagePacket msgToSend = generateInsertRequestMessage(values, msgCounter, currInsertQCounter);
        addEntryToInsertWaitQ(msgToSend, sendToPortNo, currInsertQCounter);
        //Log.i("InsertMessage", "For replica, message -- "+displayMessagePacketContents(msgToSend));
        forwardInsertRequestToNode(sendToPortNo, msgToSend);
        try {
//            Thread.sleep(TIMEOUTINTERVAL);
            Thread.sleep(TIMEOUTINTERVALINSERT);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (insertResultMap.containsKey(currInsertQCounter)) {
            //Log.i("insertResultMap", "forwardInsertRequestToReplicaNode, ACK received for "+currInsertQCounter);
        } else {
            //Log.i("insertResultMap", "forwardInsertRequestToReplicaNode, ACK not received for "+currInsertQCounter);
            msgToSend.additionalInfo = Integer.toString(Integer.parseInt(msgToSend.additionalInfo) - 1);
            String successorPort = findSuccessorPortForThisNode(sendToPortNo);
            Socket newSocket = null;
            try {
                newSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(String.valueOf((Integer.parseInt(successorPort) * 2))));
                //Log.i(taginsert, "forwardInsertRequestToReplicaNode resend after failure" + serializeMessage(msgToSend));

                BufferedWriter bw1 = new BufferedWriter(new OutputStreamWriter(newSocket.getOutputStream())); //write the message received to the output stream
                bw1.write(serializeMessage(msgToSend));
                bw1.flush();   //force invoke flush to send message and clear buffer.
                newSocket.close(); //close the socket.
            } catch (UnknownHostException e1) {
                e1.printStackTrace();
            } catch (IOException e2) {
                e2.printStackTrace();
            }
        }
//        for (int i=0;i<insertQ.size();i++) {
//            WaitQueueEntry temp = insertQ.get(i);
//            if ((temp.msgId == currInsertQCounter) && temp.status == false) {
//                //message sending failed, node down, resend to successor in chain
//                msgToSend.additionalInfo = Integer.toString(Integer.parseInt(msgToSend.additionalInfo) - 1);
//                String successorPort = findSuccessorPortForThisNode(temp.destinationPort);
//                Socket newSocket = null;
//                try {
//                    newSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//                            Integer.parseInt(String.valueOf((Integer.parseInt(successorPort) * 2))));
//                    //Log.i(taginsert, "forwardInsertRequestToReplicaNode resend after failure" + serializeMessage(msgToSend));
//
//                    BufferedWriter bw1 = new BufferedWriter(new OutputStreamWriter(newSocket.getOutputStream())); //write the message received to the output stream
//                    bw1.write(serializeMessage(msgToSend));
//                    bw1.flush();   //force invoke flush to send message and clear buffer.
//                    newSocket.close(); //close the socket.
//                } catch (UnknownHostException e1) {
//                    e1.printStackTrace();
//                } catch (IOException e2) {
//                    e2.printStackTrace();
//                }
//                break;
//            }
//        }
    }

    private void forwardInsertRequestToDestinationNode(Node destinationNode, ContentValues values) {
        int msgCounter = 3;
        int currInsertQCounter = insertQCounter;
        insertQCounter = insertQCounter + 1;
        String sendToPortNo = destinationNode.portNo;
        MessagePacket msgToSend = generateInsertRequestMessage(values, msgCounter, currInsertQCounter);
        addEntryToInsertWaitQ(msgToSend, sendToPortNo, currInsertQCounter);
        //Log.i("InsertMessage", "From original, message -- "+displayMessagePacketContents(msgToSend));
        forwardInsertRequestToNode(sendToPortNo, msgToSend);
        try {
//            Thread.sleep(TIMEOUTINTERVAL);
            Thread.sleep(TIMEOUTINTERVALINSERT);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (insertResultMap.containsKey(currInsertQCounter)) {
            //Log.i("insertResultMap", "forwardInsertRequestToDestinationNode, ACK received for "+currInsertQCounter);
        } else {
            //Log.i("insertResultMap", "forwardInsertRequestToDestinationNode, ACK not received for "+currInsertQCounter);
            msgToSend.additionalInfo = Integer.toString(Integer.parseInt(msgToSend.additionalInfo) - 1);
            String successorPort = findSuccessorPortForThisNode(sendToPortNo);
            Socket newSocket = null;
            try {
                newSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(String.valueOf((Integer.parseInt(successorPort) * 2))));
                //Log.i(taginsert, "forwardInsertRequestToDestinationNode resend after failure" + serializeMessage(msgToSend));

                BufferedWriter bw1 = new BufferedWriter(new OutputStreamWriter(newSocket.getOutputStream())); //write the message received to the output stream
                bw1.write(serializeMessage(msgToSend));
                bw1.flush();   //force invoke flush to send message and clear buffer.
                newSocket.close(); //close the socket.
            } catch (UnknownHostException e1) {
                e1.printStackTrace();
            } catch (IOException e2) {
                e2.printStackTrace();
            }
        }
//        for (int i=0;i<insertQ.size();i++) {
//            WaitQueueEntry temp = insertQ.get(i);
//
//            if ((temp.msgId == currInsertQCounter) && (temp.status == false)) {
//                //message sending failed, node down, resend to successor in chain
//                msgToSend.additionalInfo = Integer.toString(Integer.parseInt(msgToSend.additionalInfo) - 1);
//                String successorPort = findSuccessorPortForThisNode(temp.destinationPort);
//                Socket newSocket = null;
//                try {
//                    newSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//                            Integer.parseInt(String.valueOf((Integer.parseInt(successorPort) * 2))));
//                    //Log.i(taginsert, "forwardInsertRequestToDestinationNode resend after failure" + serializeMessage(msgToSend));
//
//                    BufferedWriter bw1 = new BufferedWriter(new OutputStreamWriter(newSocket.getOutputStream())); //write the message received to the output stream
//                    bw1.write(serializeMessage(msgToSend));
//                    bw1.flush();   //force invoke flush to send message and clear buffer.
//                    newSocket.close(); //close the socket.
//                } catch (UnknownHostException e1) {
//                    e1.printStackTrace();
//                } catch (IOException e2) {
//                    e2.printStackTrace();
//                }
//            }
//            break;
//        }
    }

    private void forwardInsertRequestToNode(String portNo, MessagePacket msgToSend) {
        Socket socket = null;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(String.valueOf((Integer.parseInt(portNo) * 2))));
//            socket.setSoTimeout(TIMEOUTINTERVAL);
            //Log.i(taginsert, "Sending insert request "+serializeMessage(msgToSend)+" to " + portNo);

            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); //write the message received to the output stream
            bw.write(serializeMessage(msgToSend));
            bw.flush();   //force invoke flush to send message and clear buffer.

//            addEntryToInsertWaitQ(msgToSend, portNo);
            socket.close(); //close the socket.
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void addEntryToInsertWaitQ(MessagePacket msgToSend, String destinationPortNo, int insertQCounter) {
        String tokens[] = msgToSend.message.split("\\,");
        WaitQueueEntry newEntry = new WaitQueueEntry(tokens[0], tokens[1], false, destinationPortNo, insertQCounter);
        insertQ.add(newEntry);
    }

    private String findSuccessorPortForThisNode(String portNo) {
        for (int i=0;i<nodeMap.size();i++) {
            if (nodeMap.get(i).portNo.equals(portNo)) {
                return nodeMap.get(i).succPortNo;
            }
        }
        return null;
    }

    private String findPredecessorPortForThisNode(String portNo) {
        for (int i=0;i<nodeMap.size();i++) {
            if (nodeMap.get(i).portNo.equals(portNo)) {
                return nodeMap.get(i).predPortNo;
            }
        }
        return null;
    }

    private MessagePacket generateInsertRequestMessage(ContentValues values, int msgCounter, int insertQMsgId) {
        MessagePacket msg = new MessagePacket(values.getAsString(ATTRKEY)+","+values.getAsString(ATTRVALUE), MSGTYPEINSERTREQUEST, myPort, Integer.toString(msgCounter), myPort, insertQMsgId);
        //Log.i("InsertMessage",displayMessagePacketContents(msg));
        return msg;
    }
    /////////////////////////////////END OF CODE FOR INSERT///////////////////////////////////////


    /////////////////////////////////START OF CODE FOR QUERY///////////////////////////////////////
    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
//        queryLock.lock();
        String[] attributes = {ATTRKEY, ATTRVALUE};
        MatrixCursor mCursor = new MatrixCursor(attributes);
        //Log.i("MSGQUERY", "Query request received for "+selection);
        if (selection.compareTo(SELECTIONTYPELOCAL) == 0) {
            mCursor = retrieveLocalFiles();
            //Log.i(SELECTIONTYPELOCAL, "Local Dump requested");
        } else if (selection.compareTo(SELECTIONTYPEGLOBAL) == 0) {
            for (int i=0; i<nodeMap.size(); i++) {
                MessagePacket msgToSend = generateSingleQueryRequestMessage(selection, MSGTYPEGLOBALQUERYREQUEST, -1);
                sendSingleQueryRequestToNode(nodeMap.get(i).replicaTwoPortNo, msgToSend);
            }
            try {
                Thread.sleep(1000, 0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            //Log.i("FINALGLOBAL", globalQueryContainer);
            //Log.i(MSGTYPESINGLEQUERYREQUEST, "GLOBAL --- Got back message "+mCursorGlobal);
            Cursor temp = mCursorGlobal;
            while(temp.moveToNext()) {
                //Log.i(MSGTYPESINGLEQUERYREQUEST, "GLOBAL --- Got back message with key: "+temp.getString(0)+" Value: "+temp.getString(1));
            }
            //Log.i("RETURN", "Returning cursor: "+ Arrays.toString(mCursorGlobal.getColumnNames()));
            mCursor = mCursorGlobal;
        } else {
            //Log.i("WAIT1", "SingleQueryContainer === "+singleQueryContainer);
//            while (waitingIsOver == false) {
//                //wait for it to become false
//                //Log.i("WAIT1", "Waiting in 1st loop");
//            }

            Node destinationNode = getNodeFromKey(selection);
            waitingIsOver = false;
            int currQueryQCounter = queryQCounter;
            queryQCounter = queryQCounter + 1;
            MessagePacket msgToSend = generateSingleQueryRequestMessage(selection, MSGTYPESINGLEQUERYREQUEST, currQueryQCounter);
            addEntryToQueryWaitQ(msgToSend, destinationNode.replicaTwoPortNo, currQueryQCounter);
            sendSingleQueryRequestToOriginalNode(msgToSend, destinationNode);
            //Log.i("MSGQUERY", "Query request sending msg "+serializeMessage(msgToSend));
            ///TODO code for thread sleep and query from prior node
            try {
//                Thread.sleep(TIMEOUTINTERVAL);
                Thread.sleep(TIMEOUTINTERVALQUERY);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (queryResultMap.containsKey(currQueryQCounter)) {
                //Log.i("MSGQUERY", "queryResultMap contains key "+currQueryQCounter);
                String[] tokens = queryResultMap.get(currQueryQCounter).split("\\,");
                mCursor.addRow(new String[]{tokens[0],tokens[1]});
            } else {
                String predecessorPort = destinationNode.replicaOnePortNo;
                Socket newSocket = null;
                try {
                    newSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(String.valueOf((Integer.parseInt(predecessorPort) * 2))));
                    //Log.i(taginsert, "SELECTIONTYPESINGLE query resend after failure to " +predecessorPort + " ---> " + serializeMessage(msgToSend));

                    BufferedWriter bw1 = new BufferedWriter(new OutputStreamWriter(newSocket.getOutputStream())); //write the message received to the output stream
                    bw1.write(serializeMessage(msgToSend));
                    bw1.flush();   //force invoke flush to send message and clear buffer.
                    newSocket.close(); //close the socket.
                } catch (UnknownHostException e1) {
                    e1.printStackTrace();
                } catch (IOException e2) {
                    e2.printStackTrace();
                }
                try {
//                Thread.sleep(TIMEOUTINTERVAL);
                    Thread.sleep(TIMEOUTINTERVALQUERY);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                //Log.i("MSGQUERY", "After failure reesend wake up for msgId = "+currQueryQCounter);
                if (queryResultMap.containsKey(currQueryQCounter)) {
                    //Log.i("MSGQUERY", "After failure resend, queryResultMap contains key "+currQueryQCounter);
                    String[] tokens = queryResultMap.get(currQueryQCounter).split("\\,");
                    mCursor.addRow(new String[]{tokens[0],tokens[1]});
                } else {
//                    ///Sending to original after failed to fetch from both replicas
//
                    String originalPort = destinationNode.portNo;
                    Socket newSocket1 = null;
                    try {
                        newSocket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(String.valueOf((Integer.parseInt(originalPort) * 2))));
                        //Log.i(taginsert, "SELECTIONTYPESINGLE query resend after failure to " +originalPort + " ---> " + serializeMessage(msgToSend));

                        BufferedWriter bw1 = new BufferedWriter(new OutputStreamWriter(newSocket1.getOutputStream())); //write the message received to the output stream
                        bw1.write(serializeMessage(msgToSend));
                        bw1.flush();   //force invoke flush to send message and clear buffer.
                        newSocket1.close(); //close the socket.
                    } catch (UnknownHostException e1) {
                        e1.printStackTrace();
                    } catch (IOException e2) {
                        e2.printStackTrace();
                    }
                    try {
//                Thread.sleep(TIMEOUTINTERVAL);
                        Thread.sleep(TIMEOUTINTERVALQUERY);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    //Log.i("MSGQUERY", "After failure resend wake up for msgId = "+currQueryQCounter);
                    if (queryResultMap.containsKey(currQueryQCounter)) {
                        //Log.i("MSGQUERY", "After failure resend, queryResultMap contains key "+currQueryQCounter);
                        String[] tokens = queryResultMap.get(currQueryQCounter).split("\\,");
                        mCursor.addRow(new String[]{tokens[0],tokens[1]});
                    }
                }
            }
        }
//        queryLock.unlock();
        return mCursor;
    }

    private void sendSingleQueryRequestToOriginalNode(MessagePacket msgToSend, Node destinationNode) {
        sendSingleQueryRequestToNode(destinationNode.replicaTwoPortNo, msgToSend);
    }

    private void addEntryToQueryWaitQ(MessagePacket msgToSend, String destinationPortNo, int queryQCounter) {
        String tokens[] = msgToSend.message.split("\\,");
        String msgKey = tokens[0];
        String msgVal = "NULL";
        WaitQueueEntry newEntry = new WaitQueueEntry(msgKey, msgVal, false, destinationPortNo, queryQCounter);
        queryQ.add(newEntry);
    }

    private void sendSingleQueryRequestToNode(String portNo, MessagePacket msgToSend) {
        Socket socket = null;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(String.valueOf((Integer.parseInt(portNo) * 2))));
//            socket.setSoTimeout(TIMEOUTINTERVAL);
            //Log.i(tagdelete, "Sending single query request to "+portNo + "---->"+serializeMessage(msgToSend));

            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); //write the message received to the output stream
            bw.write(serializeMessage(msgToSend));
            bw.flush();   //force invoke flush to send message and clear buffer.
            socket.close(); //close the socket.
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private MessagePacket generateSingleQueryRequestMessage(String selection, String msgType, int msgId) {
        MessagePacket msg = new MessagePacket(selection, msgType, myPort, "NULL", myPort, msgId);
        return msg;
    }

    private MatrixCursor retrieveLocalFiles() {
        String[] attributes = {ATTRKEY, ATTRVALUE};
        String messages = "";
        MatrixCursor mCursor = new MatrixCursor(attributes);
        String[] fileList = getContext().fileList();
        String dataRead = "";
        for (int i=0;i<fileList.length;i++) {
            try {
                String filepath = getContext().getFilesDir().getAbsolutePath();
                BufferedReader br = new BufferedReader(new FileReader(new File(filepath, fileList[i])));
                dataRead = br.readLine();
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            globalQueryContainer = globalQueryContainer + fileList[i] + "," + dataRead + "*";
            mCursor.addRow(new String[]{fileList[i],dataRead});
        }
        return mCursor;
    }


    /////////////////////////////////END OF CODE FOR QUERY///////////////////////////////////////


    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    /////////////////////////////////START OF CODE FOR ONCREATE AND ITS HELPER FUNCTIONS///////////////////////////////////////
    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = portStr;
        try {
            myNodeHashId = genHash(myPort);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        //Log.i("oncreate", "Inside oncreate for "+myPort);
        nodeMap = new ArrayList<Node>();
        myReplicaNodePortNos = new String[2];
//        waitQueue = new HashMap<String, WaitQueueEntry>();
        Uri.Builder URI_builder = new Uri.Builder();
        URI_builder.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
        URI_builder.scheme("content");
        URI_obj = URI_builder.build();
        ATTRKEY = new String("key");
        ATTRVALUE = new String("value");
        //Log.i(taggeneral, "MYPORT == "+myPort);
        generateNodeMap();
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            //Log.i(taggeneral, "Cannot invoke ServerTask!!!");
            e.printStackTrace();
        }
        requestLostKeys();
        singleQueryContainer = "";
        globalQueryContainer = "";

        mCursorGlobal = new MatrixCursor(new String[] {ATTRKEY, ATTRVALUE});
        insertQ = new ArrayList<WaitQueueEntry>();
        queryQ = new ArrayList<WaitQueueEntry>();
        queryResultMap = new HashMap<Integer, String>();
        insertResultMap = new HashMap<Integer, Boolean>();
        insertQCounter = 0;
        queryQCounter = 0;
        waitingIsOver = true;

        queryLock = new ReentrantLock();
        insertLock = new ReentrantLock();
        deleteLock = new ReentrantLock();
        //Log.i("oncreate", "Leaving oncreate for "+myPort);
        return false;
    }

    public void generateNodeMap() {
        for (int i=0;i<remote_ports.length;i++) {
            addNewNode(remote_ports[i]);
        }
        setNodeReplicas();
        displayNodeMap();
    }

    private void setNodeReplicas() {
        for (int i=0;i<nodeMap.size();i++) {
            nodeMap.get(i).replicaOnePortNo = nodeMap.get((i+1)%5).portNo;
            nodeMap.get(i).replicaTwoPortNo = nodeMap.get((i+2)%5).portNo;
            if (nodeMap.get(i).portNo.equals(myPort)) {
                myReplicaNodePortNos[0] = nodeMap.get(i).replicaOnePortNo;
                myReplicaNodePortNos[1] = nodeMap.get(i).replicaTwoPortNo;
                predPort = nodeMap.get(i).predPortNo;
                succPort = nodeMap.get(i).succPortNo;
            }
        }
    }

    public void requestLostKeys() {
        //Log.i("lostKey", "Inside requestLostKeys");
        String predPredPort = "";
        String succSuccPort = "";
        for (int i=0;i<nodeMap.size();i++) {
            if (nodeMap.get(i).replicaTwoPortNo.equals(myPort)) {
                predPredPort = nodeMap.get(i).portNo;
                break;
            }
        }

        for (int i=0;i<nodeMap.size();i++) {
            if (myReplicaNodePortNos[1].equals(nodeMap.get(i).portNo)) {
                succSuccPort = nodeMap.get(i).portNo;
                break;
            }
        }
        //Log.i("REQLOST", "Requesting Lost keys from succport: "+succPort+", predport: "+predPort+ " , predPredPort: "+predPredPort + ", succSuccPort: "+succSuccPort);
        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, succPort);
        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, predPort);
        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, predPredPort);
        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, succSuccPort);
    }

//    public void requestLostKeyFromNode(String portNo) {
//        //Log.i("lostKey", "Sending requestLostKeys to "+portNo);
//        Socket socket1 = null;
//        try {
//            socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//                    Integer.parseInt(String.valueOf((Integer.parseInt(portNo) * 2))));
//            //Log.i(taggeneral, "Sending Lost key request message to " + portNo);
//            MessagePacket msgToSend = generateLostKeyRequestMessage();
//            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket1.getOutputStream())); //write the message received to the output stream
//            bw.write(serializeMessage(msgToSend));
//            bw.flush();   //force invoke flush to send message and clear buffer.
//            socket1.close(); //close the socket.
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    public MessagePacket generateLostKeyRequestMessage() {
        MessagePacket msgToSend = new MessagePacket("NULL", MSGTYPELOSTKEYREQUEST, myPort, "NULL", myPort, -1);
        return  msgToSend;

    }

    public void addNewNode(String newNodePort) {
        String key = null;
        try {
            key = genHash(newNodePort);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        Node newNode = new Node();
        newNode.hashId = key;
        newNode.portNo = newNodePort;
        newNode.predPortNo = "";
        newNode.succPortNo = "";
        Node currNode;
        if (nodeMap.size() == 0) {
            newNode.predPortNo = newNode.portNo;
            newNode.succPortNo = newNode.portNo;
            nodeMap.add(newNode);
            headNode = newNode;
            headNodePort = newNodePort;
        } else if (nodeMap.size() == 1) {
            currNode = nodeMap.get(0);
            if (newNode.hashId.compareTo(currNode.hashId) > 0) {
                newNode.predPortNo = currNode.portNo;
                newNode.succPortNo = currNode.portNo;
                currNode.predPortNo = newNode.portNo;
                currNode.succPortNo = newNode.portNo;
                nodeMap.set(0, currNode);
                nodeMap.add(newNode);
            } else if (newNode.hashId.compareTo(currNode.hashId) < 0) {
                newNode.predPortNo = currNode.portNo;
                newNode.succPortNo = currNode.portNo;
                currNode.predPortNo = newNode.portNo;
                currNode.succPortNo = newNode.portNo;
                nodeMap.add(0, newNode);
                nodeMap.set(1, currNode);
                headNode = newNode;
                headNodePort = newNodePort;
            }
        } else {
            for (int i=0;i<nodeMap.size();i++) {
                currNode = nodeMap.get(i);
                try {
                    if (currNode.portNo.equals(headNodePort) && newNode.hashId.compareTo(currNode.hashId) < 0 && newNode.hashId.compareTo(genHash(currNode.predPortNo)) < 0) {
                        newNode.succPortNo = currNode.portNo;
                        newNode.predPortNo = currNode.predPortNo;
                        nodeMap.get(nodeMap.size() - 1).succPortNo = newNode.portNo;
                        currNode.predPortNo = newNode.portNo;
                        nodeMap.add(i, newNode);
                        nodeMap.set(i+1, currNode);
                        headNodePort = newNodePort;
                        headNode = newNode;
                    } else if (newNode.hashId.compareTo(currNode.hashId) > 0 && newNode.hashId.compareTo(genHash(currNode.succPortNo)) > 0 && (i == (nodeMap.size() - 1))) {
                        newNode.succPortNo = currNode.succPortNo;
                        newNode.predPortNo = currNode.portNo;
                        currNode.succPortNo = newNode.portNo;
                        nodeMap.get(0).predPortNo = newNode.portNo;
                        nodeMap.add(newNode);
                        nodeMap.set(i, currNode);
                    } else if (newNode.hashId.compareTo(currNode.hashId) > 0 && newNode.hashId.compareTo(genHash(currNode.succPortNo)) < 0) {
                        newNode.predPortNo = currNode.portNo;
                        newNode.succPortNo = currNode.succPortNo;
                        nodeMap.get(i).succPortNo = newNode.portNo;
                        nodeMap.get(i+1).predPortNo = newNode.portNo;
                        nodeMap.add(i+1, newNode);
                    }
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }

            }
        }
    }

    private void displayNodeMap() {
        String message = "";
        Node temp = new Node();
        for (int i=0;i<nodeMap.size();i++) {
            temp = nodeMap.get(i);
            message += "Node_" + i + ", Hash = "+temp.hashId + ", portNo = "+temp.portNo + " , predPortNo = "+temp.predPortNo + ", succPortNo = "+temp.succPortNo + " , ReplicaOnePortNo = "+temp.replicaOnePortNo + ", ReplicaTwoPortNo = "+temp.replicaTwoPortNo + " -----> ";
        }
        //Log.i("Nodemap_displayy", "Nodemap ======>> " + message);
        //Log.i("headNode_displayy", "HeadNode = "+headNodePort);

    }

    private String displayMessagePacketContents(MessagePacket msgPacket) {
        StringBuilder tmp = new StringBuilder();
        tmp.append("Message :");
        tmp.append(msgPacket.message);
        tmp.append(", Message Type:");
        tmp.append(msgPacket.msgType);
        tmp.append(", Sender :");
        tmp.append(msgPacket.senderPort);
        tmp.append(", Additional :");
        tmp.append(msgPacket.additionalInfo);
        return tmp.toString();
    }
    /////////////////////////////////END OF CODE FOR ONCREATE AND ITS HELPER FUNCTIONS///////////////////////////////////////


    /////////////////////////////////START OF CODE FOR GENERAL HELPER FUNCTIONS/////////////////////////////////////////////
    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private boolean checkIfHeadNode(String portNo) {
        if (portNo.equals(headNodePort)) {
            return true;
        } else {
            return false;
        }
    }

    private String serializeMessage(MessagePacket m) {
        String selMsg = "";
        selMsg = m.message + "|" + m.msgType + "|" + m.senderPort + "|" + m.additionalInfo + "|" + m.fromPort + "|" +m.msgId;
        return selMsg;
    }

    private MessagePacket unserializeMessage(String msg) {
        if (msg == null) {
            //Log.i("unserialize", "null message received");
        } else {
            //Log.i("unserialize", "Message = "+msg);
        }
        String[] tokens = msg.split("\\|");
        MessagePacket m;
        m = new MessagePacket(tokens[0], tokens[1], tokens[2], tokens[3], tokens[4], Integer.parseInt(tokens[5]));
        return m;
    }

    private Node getNodeFromKey(String selection) {
        try {
            String hashKey = genHash(selection);
            Node currNode = new Node();
            for (int i=0;i<nodeMap.size();i++) {
                currNode = nodeMap.get(i);
                if (checkIfHeadNode(currNode.portNo)) {
                    if ((hashKey.compareTo(genHash(currNode.predPortNo)) > 0) || (hashKey.compareTo(genHash(currNode.portNo)) <= 0)) {
                        return currNode;
                    }
                } else {
                    if (hashKey.compareTo(genHash(currNode.portNo)) <= 0 && hashKey.compareTo(genHash(currNode.predPortNo)) > 0) {
                        return currNode;
                    }
                }
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    private boolean checkIfMessageBelongsToNode(String key, String nodePortNo) {
        boolean belongsToNode = false;
        try {
            String hashKey = genHash(key);
            Node currNode = getNodeFromPortNo(nodePortNo);
            if (checkIfHeadNode(currNode.portNo)) {
                if ((hashKey.compareTo(genHash(currNode.predPortNo)) > 0) || (hashKey.compareTo(genHash(currNode.portNo)) <= 0)) {
                    belongsToNode = true;
                }
            } else {
                if (hashKey.compareTo(genHash(currNode.portNo)) <= 0 && hashKey.compareTo(genHash(currNode.predPortNo)) > 0) {
                    belongsToNode = true;
                }
            }
        } catch(NoSuchAlgorithmException e){
            e.printStackTrace();
        }
        return belongsToNode;
    }

    private Node getNodeFromPortNo(String nodePortNo) {
        for (int i=0;i<nodeMap.size();i++) {
            if (nodePortNo.equals(nodeMap.get(i).portNo)) {
                return nodeMap.get(i);
            }
        }
        return null;
    }

    private void sendSingleQueryDataReadToRequester(String dataRead, MessagePacket msgPacket) {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(String.valueOf((Integer.parseInt(msgPacket.senderPort) * 2))));
            MessagePacket msgToSend = generateQueryResultNotifyMessageForSingleRequest(dataRead, msgPacket);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); //write the message received to the output stream
            //Log.i("MSQR", "Sending back "+serializeMessage(msgToSend)+" to port: "+msgPacket.senderPort);
            bw.write(serializeMessage(msgToSend));
            bw.flush();   //force invoke flush to send message and clear buffer.
            socket.close(); //close the socket.
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private MessagePacket generateQueryResultNotifyMessageForSingleRequest(String dataRead, MessagePacket msgPacket) {
        MessagePacket msg = new MessagePacket(msgPacket.message+","+dataRead, MSGTYPESINGLEQUERYRESPONSE, myPort, "NULL", myPort, Integer.parseInt(msgPacket.msgId));
        return msg;
    }

    private void sendAllMyMessagesToRequester(String allMessages, MessagePacket msgPacket, int length, String msgType) {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(String.valueOf((Integer.parseInt(msgPacket.fromPort) * 2))));
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); //write the message received to the output stream
            MessagePacket msgToSend = generateMessagePacketForGlobalQueryRequest(allMessages, length, msgType);
            //Log.i(taginsert, "Sending all my keys to "+msgPacket.fromPort + "----->" + msgToSend);
            bw.write(serializeMessage(msgToSend));
            bw.flush();   //force invoke flush to send message and clear buffer.
            socket.close(); //close the socket.
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private MessagePacket generateMessagePacketForGlobalQueryRequest(String dataRead, int length, String msgType) {
        MessagePacket msg = new MessagePacket(dataRead, msgType, myPort, String.valueOf(length), myPort, -1);
        return msg;
    }

    /////////////////////////////////END OF CODE FOR HELPER FUNCTIONS///////////////////////////////////////////////

    /////////////////////////////////START OF CODE FOR SERVERTASK///////////////////////////////////////
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        protected Void doInBackground(ServerSocket... sockets) {
            //Log.i(taggeneral, "Inside ServerTask for ---"+myPort);
            ServerSocket serverSocket = sockets[0];
            Socket sock = null;
            String message;
            while (true) {
                try {
                    //Log.i(taggeneral, "Inside servertask for " + myPort);
                    sock = serverSocket.accept();
                    BufferedReader br = new BufferedReader(new InputStreamReader(sock.getInputStream()));   //read message from input stream.
                    message = br.readLine();
                    new ServerHelperTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                } catch (IOException err) {
                    Log.e("ServerTask", "Server failed");
                }
            }
        }
    }

    private void sendAckToOriginatorNode(MessagePacket msgPacket) {
        MessagePacket msgToSend = generateAckMessageForInsert(msgPacket);
        String destinationNode = msgPacket.fromPort;
        Socket socket1 = null;
        try {
            socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(String.valueOf((Integer.parseInt(destinationNode) * 2))));
            //Log.i(taggeneral, "Sending ACK to " + destinationNode + " for message" + serializeMessage(msgPacket));
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket1.getOutputStream())); //write the message received to the output stream
            bw.write(serializeMessage(msgToSend));
            bw.flush();   //force invoke flush to send message and clear buffer.
            socket1.close(); //close the socket.
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private MessagePacket generateAckMessageForInsert(MessagePacket msgPacket) {
        MessagePacket m = new MessagePacket(msgPacket.message, MSGTYPEINSERTACK, myPort, msgPacket.additionalInfo, myPort, Integer.parseInt(msgPacket.msgId));
        return m;
    }

    /////////////////////////////////END OF CODE FOR SERVERTASK///////////////////////////////////////

    /////////////////////////////////START OF CODE FOR CLIENTTASK///////////////////////////////////////
    private class ClientTask extends AsyncTask<String, Void, Void> {
        protected Void doInBackground(String... msgs) {
            Socket socket1 = null;
            try {
                String portNo = msgs[0];
                socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(String.valueOf((Integer.parseInt(portNo) * 2))));
                //Log.i(taggeneral, "Sending Lost key request message to " + portNo);
                MessagePacket msgToSend = generateLostKeyRequestMessage();
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket1.getOutputStream())); //write the message received to the output stream
                bw.write(serializeMessage(msgToSend));
                bw.flush();   //force invoke flush to send message and clear buffer.
                socket1.close(); //close the socket.
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    /////////////////////////////////END OF CODE FOR CLIENTTASK///////////////////////////////////////

    private class ServerHelperTask extends AsyncTask<String, String, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String message = msgs[0];
            //Log.i(taggeneral, "Inside ServerHelperTask, received message --->" + message);
            MessagePacket msgPacket = unserializeMessage(message);

            ////////////////////////START OF HANDLING DELETE OPERATIONS////////////////////////////////////
            if (msgPacket.msgType.equals(MSGTYPELOCALDEL)) {
                String[] localFileList = getContext().fileList();
//                        Node tempNode = new Node();
                for (int i = 0; i < localFileList.length; i++) {
                    if (checkIfMessageBelongsToNode(localFileList[i], msgPacket.senderPort)) {
                        getContext().deleteFile(localFileList[i]);
                    }
                }
            } else if (msgPacket.msgType.equals(MSGTYPEGLOBALDEL)) {
                deleteLocalFiles();
            } else if (msgPacket.msgType.equals(MSGTYPESINGLEDEL)) {
                getContext().deleteFile(msgPacket.message);
            }
            ////////////////////////END OF HANDLING DELETE OPERATIONS/////////////////////////////////////


            ////////////////////////START OF HANDLING INSERT OPERATION////////////////////////////////////
            else if (msgPacket.msgType.equals(MSGTYPEINSERTREQUEST)) {
                String tokens[] = msgPacket.message.split("\\,");
                String msgKey = tokens[0];
                String msgValue = tokens[1];
                //Log.i(taginsert, "INSERTREQUEST received --> key = "+msgKey + " with value = "+msgValue);
                String filePath = getContext().getFilesDir().getAbsolutePath();
                try {
                    FileWriter fw = new FileWriter(new File(filePath, msgKey));
                    fw.write(msgValue);
                    fw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                sendAckToOriginatorNode(msgPacket);
                int counter = Integer.parseInt(msgPacket.additionalInfo) - 1;
                if (counter != 0) {
                    msgPacket.additionalInfo = Integer.toString(counter);
                    msgPacket.fromPort = myPort;
                    int currInsertQCounter = insertQCounter;
                    insertQCounter = insertQCounter + 1;
                    msgPacket.msgId = Integer.toString(currInsertQCounter);
                    addEntryToInsertWaitQ(msgPacket, succPort, currInsertQCounter);
                    forwardInsertRequestToNode(succPort, msgPacket);
                    try {
//                        Thread.sleep(TIMEOUTINTERVAL);
                        Thread.sleep(TIMEOUTINTERVALINSERT);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if (insertResultMap.containsKey(currInsertQCounter)) {
                        //Log.i("insertResultMap", "ServerHelperTask, ACK received for "+currInsertQCounter);
                    } else {
                        //Log.i("insertResultMap", "ServerHelperTask, ACK not received for "+currInsertQCounter);
                        msgPacket.additionalInfo = Integer.toString(Integer.parseInt(msgPacket.additionalInfo) - 1);
                        if (Integer.parseInt(msgPacket.additionalInfo) > 0) {
                            String successorPort = findSuccessorPortForThisNode(succPort);
                            Socket newSocket = null;
                            try {
                                newSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(String.valueOf((Integer.parseInt(successorPort) * 2))));
                                //Log.i(taginsert, "SERVERTASKINSERT resend after failure" + successorPort);

                                BufferedWriter bw1 = new BufferedWriter(new OutputStreamWriter(newSocket.getOutputStream())); //write the message received to the output stream
                                bw1.write(serializeMessage(msgPacket));
                                bw1.flush();   //force invoke flush to send message and clear buffer.
                                newSocket.close(); //close the socket.
                            } catch (UnknownHostException e1) {
                                e1.printStackTrace();
                            } catch (IOException e2) {
                                e2.printStackTrace();
                            }
                        }
                    }
//                    for (int i=0;i<insertQ.size();i++) {
//                        WaitQueueEntry temp = insertQ.get(i);
//                        if ((temp.msgId == currInsertQCounter) && temp.status == false) {
//                            //message sending failed, node down, resend to successor in chain
//                            msgPacket.additionalInfo = Integer.toString(Integer.parseInt(msgPacket.additionalInfo) - 1);
//                            if (Integer.parseInt(msgPacket.additionalInfo) > 0) {
//                                String successorPort = findSuccessorPortForThisNode(temp.destinationPort);
//                                Socket newSocket = null;
//                                try {
//                                    newSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//                                            Integer.parseInt(String.valueOf((Integer.parseInt(successorPort) * 2))));
//                                    //Log.i(taginsert, "SERVERTASKINSERT resend after failure" + successorPort);
//
//                                    BufferedWriter bw1 = new BufferedWriter(new OutputStreamWriter(newSocket.getOutputStream())); //write the message received to the output stream
//                                    bw1.write(serializeMessage(msgPacket));
//                                    bw1.flush();   //force invoke flush to send message and clear buffer.
//                                    newSocket.close(); //close the socket.
//                                } catch (UnknownHostException e1) {
//                                    e1.printStackTrace();
//                                } catch (IOException e2) {
//                                    e2.printStackTrace();
//                                }
//                            }
//                            break;
//                        }
//                    }
                }
            } else if (msgPacket.msgType.equals(MSGTYPEINSERTACK)) {
                //Log.i("TAGACK", "INSACK received --- >"+serializeMessage(msgPacket));
                String tokens[] = msgPacket.message.split("\\,");
                String msgKey = tokens[0];
                for (int i=0;i<insertQ.size();i++) {
                    WaitQueueEntry temp = insertQ.get(i);
                    if (temp.msgId == Integer.parseInt(msgPacket.msgId)) {
                        insertQ.get(i).status = true;
                    }
                }
                insertResultMap.put(Integer.parseInt(msgPacket.msgId), true);
            }
            ////////////////////////END OF HANDLING INSERT OPERATION////////////////////////////////////


            ////////////////////////START OF HANDLING QUERY REQUEST OPERATIONS////////////////////////////////////
            else if (msgPacket.msgType.equals(MSGTYPESINGLEQUERYREQUEST)) {
                String dataRead = "";
                try {
                    String filePath = getContext().getFilesDir().getAbsolutePath();
                    BufferedReader brq = new BufferedReader(new FileReader(new File(filePath, msgPacket.message)));
                    dataRead = brq.readLine();
                    sendSingleQueryDataReadToRequester(dataRead, msgPacket);
                    brq.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else if (msgPacket.msgType.equals(MSGTYPEGLOBALQUERYREQUEST)) {
                //Log.i(MSGTYPEGLOBALQUERYREQUEST, "Global Dump requested by " + msgPacket.senderPort);
                String dataRead = "";
                String allMessages = "";
                String[] localFileList = getContext().fileList();
                for (int i = 0; i < localFileList.length; i++) {
                    try {
                        String filePath = getContext().getFilesDir().getAbsolutePath();
                        BufferedReader brq = new BufferedReader(new FileReader(new File(filePath, localFileList[i])));
                        dataRead = brq.readLine();
                        allMessages = allMessages + localFileList[i] + "," + dataRead + "*";
                        brq.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                //Log.i(MSGTYPEGLOBALQUERYREQUEST, "All My Messages sending back to " + msgPacket.fromPort + "--->"+allMessages);
                sendAllMyMessagesToRequester(allMessages, msgPacket, localFileList.length, MSGTYPEGLOBALQUERYRESPONSE);
            }
            ////////////////////////END OF HANDLING QUERY REQUEST OPERATION/////////////////////////////////////////



            ////////////////////////START OF HANDLING QUERY RESPONSE OPERATIONS//////////////////////////////////////
            else if (msgPacket.msgType.equals(MSGTYPESINGLEQUERYRESPONSE)) {
                String[] tokens = msgPacket.message.split("\\,");
                //Log.i(MSGTYPESINGLEQUERYRESPONSE, "Got Queried message back ----> " +serializeMessage(msgPacket));
                singleQueryContainer = msgPacket.message;
                waitingIsOver = true;
                String msgKey = tokens[0];
                queryResultMap.put(Integer.parseInt(msgPacket.msgId), msgPacket.message);
//                for (int i=0;i<queryQ.size();i++) {
//                    WaitQueueEntry temp = queryQ.get(i);
//                    //Log.i(MSGTYPESINGLEQUERYRESPONSE, "temp.key: " + temp.key + ", msgKey: " + msgKey + ", temp.msgId: "+temp.msgId + ", msgPacket.msgId: " + msgPacket.msgId +" from " + msgPacket.senderPort);
//                    if (temp.key.equals(msgKey) && temp.msgId == Integer.parseInt(msgPacket.msgId)) {
//                        //Log.i(MSGTYPESINGLEQUERYRESPONSE, "Got Queried INSIDEIF: " + tokens[0] + ", value: " + tokens[1] + " from " + msgPacket.senderPort);
//                        queryQ.get(i).status = true;
//                        queryQ.get(i).value = tokens[1];
//                    } else {
//                        //Log.i(MSGTYPESINGLEQUERYRESPONSE, "QueryQELSE -  " + temp.msgId+ " , "+temp.status);
//                    }
//                }
//                        waitQueue.get(tokens[0]).value = msgPacket.message;
//                        waitQueue.get(tokens[0]).status = true;
            }
            else if (msgPacket.msgType.equals(MSGTYPEGLOBALQUERYRESPONSE)) {
                //Log.i("COUNTOFMSG", "Received Query Response from " + msgPacket.senderPort + " and messageCount = " + msgPacket.additionalInfo);
                //Log.i(MSGTYPEGLOBALQUERYRESPONSE, "Messages = " + msgPacket.message);
                globalQueryContainer = globalQueryContainer + msgPacket.message;
                String[] msgList = msgPacket.message.split("\\*");
                for (int i = 0; i < msgList.length; i++) {
                    if (msgList[i] == null) {
                        //Log.i("NULLMSG", "NULL message do nothing");
                        //do nothing
                    } else {
                        String tokens[] = msgList[i].split("\\,");
                        if (tokens.length == 1) {
                            //Log.i("SINGLETOKEN", "SINGLETOKEN do nothing");
                            //d0 nothing
                        } else {
                            mCursorGlobal.addRow(new String[]{tokens[0], tokens[1]});
                        }
                    }
                }
//                for (int i=0;i<queryQ.size();i++) {
//                    WaitQueueEntry temp = queryQ.get(i);
//                    if (temp.key.equals(SELECTIONTYPEGLOBAL) && temp.msgId == Integer.parseInt(msgPacket.msgId) && temp.destinationPort.equals(msgPacket.fromPort)) {
//                        queryQ.get(i).status = true;
//                    }
//                }
            }

            ////////////////////////END OF HANDLING QUERY RESPONSE OPERATIONS////////////////////////////////////////

            ////////////////////////START OF HANDLING LOST KEY REQUEST OPERATIONS//////////////////////////////////////
            else if (msgPacket.msgType.equals(MSGTYPELOSTKEYREQUEST)) {
                //Log.i(MSGTYPELOSTKEYREQUEST, "Lost keys requested by " + msgPacket.senderPort);
                String dataRead = "";
                String allMessages = "";
                String[] localFileList = getContext().fileList();
                for (int i = 0; i < localFileList.length; i++) {
//                    if (checkIfMessageBelongsToNode(localFileList[i], myPort)) {
                        try {
                            String filePath = getContext().getFilesDir().getAbsolutePath();
                            BufferedReader brq = new BufferedReader(new FileReader(new File(filePath, localFileList[i])));
                            dataRead = brq.readLine();
                            allMessages = allMessages + localFileList[i] + "," + dataRead + "*";
                            brq.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
//                    }
                }
                //Log.i(MSGTYPELOSTKEYREQUEST, MSGTYPELOSTKEYREQUEST+" - All My Messages sending back to " + msgPacket.fromPort + "--->"+allMessages);
                sendAllMyMessagesToRequester(allMessages, msgPacket, localFileList.length, MSGTYPELOSTKEYRESPONSE);
            }
            ////////////////////////END OF HANDLING LOST KEY REQUEST OPERATIONS//////////////////////////////////////

            ////////////////////////START OF HANDLING LOST KEY RESPONSE OPERATIONS//////////////////////////////////////
            else if (msgPacket.msgType.equals(MSGTYPELOSTKEYRESPONSE)) {
                //Log.i(MSGTYPELOSTKEYRESPONSE, "LOSTKEYRESSPONSE from "+msgPacket.fromPort + " with messagecount = "+msgPacket.additionalInfo);
                //Log.i(MSGTYPELOSTKEYRESPONSE, "LOSTKEYRESSPONSE messagea ----> "+msgPacket.message);
                String[] msgList = msgPacket.message.split("\\*");
                for (int i = 0; i < msgList.length; i++) {
                    //Log.i("RecoveredMSG", "Outside, Recovered message from -"+msgList[i]);
                    if (msgList[i] == null) {
                        //Log.i("NULLMSG", "NULL message do nothing");
                        //do nothing
                    } else {
                        String tokens[] = msgList[i].split("\\,");
                        if (tokens.length == 1) {
                            //Log.i("SINGLETOKEN", "SINGLETOKEN do nothing");
                            //d0 nothing
                        } else {
                            String msgKey = tokens[0];
                            String msgValue = tokens[1];
                            Node destinationNode = getNodeFromKey(msgKey);
                            //Log.i("RecoveredMSG", "RecoveredMSG from " + msgPacket.senderPort + " --> "+msgKey + " with value = "+msgValue+", destined for "+destinationNode.portNo);
                            if ((destinationNode.portNo.equals(myPort)) || (destinationNode.replicaOnePortNo.equals(myPort)) || (destinationNode.replicaTwoPortNo.equals(myPort))) {
                                //Log.i("RecoveredMSG", "Inserting recovered message from " + msgPacket.senderPort + " --> "+msgKey + " with value = "+msgValue);
                                String filePath = getContext().getFilesDir().getAbsolutePath();
                                try {
                                    FileWriter fw = new FileWriter(new File(filePath, msgKey));
                                    fw.write(msgValue);
                                    fw.close();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                }
            }
            ////////////////////////END OF HANDLING LOST KEY RESPONSE OPERATIONS//////////////////////////////////////
            return null;
        }
    }
}