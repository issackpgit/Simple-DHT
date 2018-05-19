package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();

    private DBHelper dbHelper;
    String myPort;
    String hashID;
    String key;
    String value;
    String pred="";
    String succ="";
    String outKey="";
    String outVal="";
    String firstPort = "11108";
    String[] portList = {"5554","5556","5558","5560","5562"};
    static final int SERVER_PORT = 10000;

    TreeMap<String, String> ring = new TreeMap<String, String>();
    TreeMap<String,String> sortedList = new TreeMap<String,String>();

    private HashMap<String,String> outputStore = new HashMap();
    private HashMap<String,String> starStore = new HashMap();


    boolean flag =false;


    LinkedList<Message> msgObjects = new LinkedList<Message>();
    Message temp = new Message();

    Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        SQLiteDatabase db = dbHelper.getWritableDatabase();
        int rowsDeleted=0;
        String myHash="";
        String hashKey = "";
        try {
            hashKey = genHash(selection);
            myHash = genHash(myPort);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if(pred.equals("")&&succ.equals("")) {
            if (selection.equals("*"))
                rowsDeleted = db.delete(dbHelper.TABLE_NAME, null, null);
            else if (selection.equals("@"))
                rowsDeleted = db.delete(dbHelper.TABLE_NAME, null, null);
            else
                rowsDeleted = db.delete(dbHelper.TABLE_NAME, "key = ?", new String[] {selection});
        }
        else{
            System.out.println("Inside the delete function");

            if(selection.equals("*")){
                System.out.println("Inside the * delete ");
                rowsDeleted = db.delete(dbHelper.TABLE_NAME, null, null);
                Message msg = new Message("Delete",myPort);
                msg.setKey(selection);
                msg.setSucc(succ);
                msg.setPred(pred);
                msg.setSendPort(myPort);

                try {
                    sendDelete(msg);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
            else if (selection.equals("@")){
                System.out.println("Inside the @ delete ");
                rowsDeleted = db.delete(dbHelper.TABLE_NAME, null, null);
            }
            else {

//                rowsDeleted = db.delete(dbHelper.TABLE_NAME, "key = ?", new String[] {selection});
                System.out.println("Rows Deleted:"+rowsDeleted);

                if(hashKey.compareTo(myHash)<0 && hashKey.compareTo(pred)>=0){
                    //own key and value
                    Log.e(TAG,"Delete 1");

                    rowsDeleted = db.delete(dbHelper.TABLE_NAME, "key = ?", new String[] {selection});
                    Log.e("Delete successfull","in Delete 1");
                }
                else if(myHash.compareTo(succ)<0 && myHash.compareTo(pred)<0 ){
                    //First node in ring
                    if(hashKey.compareTo(pred)>=0 || hashKey.compareTo(myHash)<0) {
                        Log.e(TAG,"Delete 2");
                        rowsDeleted = db.delete(dbHelper.TABLE_NAME, "key = ?", new String[] {selection});
                    }
                    else{
                        Log.e(TAG,"Delete 3");
                        Message msg = new Message("Delete",myPort);
                        msg.setKey(selection);
                        msg.setSucc(succ);
                        msg.setPred(pred);
                        msg.setSendPort(myPort);

                        try {
                            sendDelete(msg);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
                else{
                    //Send to successor
                    Log.e(TAG,"Delete 4");
                    Message msg = new Message("Delete",myPort);
                    msg.setKey(selection);
                    msg.setSucc(succ);
                    msg.setPred(pred);
                    msg.setSendPort(myPort);

                    try {
                        sendDelete(msg);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }

            }

        }
        return rowsDeleted;
    }

    private void sendDelete(Message msg) throws IOException {
        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ring.get(msg.getSucc())));
        OutputStream out = socket.getOutputStream();
        DataOutputStream writer = new DataOutputStream(out);
        writer.writeUTF(msg.toString());
        out.close();
        writer.close();
        socket.close();
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        SQLiteDatabase db = dbHelper.getWritableDatabase();
        String myHash="";
        String hashKey ="";

        Log.v("In", "Insert Values");

        key = (String) values.get("key");
        value = (String) values.get("value");


        try {
            findSuccandPred(myPort);
            hashKey = genHash(key);
            myHash = genHash(myPort);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        Log.i("Successor:",succ);
        Log.i("Predeccor:",pred);

        if(succ.equals("")&& pred.equals("")){
            db.insertWithOnConflict(dbHelper.TABLE_NAME, null, values, db.CONFLICT_REPLACE);
            Log.v("insert", "successfull");
        }
        else{
            Log.e(TAG,"Else condition of insert");

            if(hashKey.compareTo(myHash)<0 && hashKey.compareTo(pred)>=0){
                //own key and value
                Log.e(TAG,"Here 1");

                db.insertWithOnConflict(dbHelper.TABLE_NAME, null, values, db.CONFLICT_REPLACE);
                Log.e("Insertion successfull","in Here 1");
            }
            else if(myHash.compareTo(succ)<0 && myHash.compareTo(pred)<0 ){
                //First node in ring
                if(hashKey.compareTo(pred)>=0 || hashKey.compareTo(myHash)<0) {
                    db.insertWithOnConflict(dbHelper.TABLE_NAME, null, values, db.CONFLICT_REPLACE);
                    Log.e(TAG,"Here 2");
                }
                else{
                    try {
                        sendInsertRequest(values, ring.get(succ) , ring.get(pred));
                        Log.e(TAG,"Here 3");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            else{
                //Send to successor
                try {
                    Log.e(TAG,"Here 4");
                    Log.e("Succ in 4",ring.get(succ));
                    Log.e("Msg to succ", (String) values.get("key"));
                    sendInsertRequest(values, ring.get(succ),ring.get(pred));

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

        return uri;

    }

    private void sendInsertRequest(ContentValues values, String suc, String pre) throws IOException {
        Message msgToSend = new Message();
        msgToSend.setPort(myPort);
        msgToSend.setSucc(suc);
        msgToSend.setType("Insert");
        msgToSend.setPred(pre);
        msgToSend.setValues(values);
        Log.e("Sending request to succ", suc);
        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(suc));
        OutputStream out = socket.getOutputStream();
        DataOutputStream writer = new DataOutputStream(out);
        writer.writeUTF(msgToSend.toString());
        out.close();
        writer.close();
        socket.close();

    }
    private void sendRequesttoPred(ContentValues values, String suc, String pre) throws IOException {
        Message msgToSend = new Message();
        msgToSend.setPort(myPort);
        msgToSend.setSucc(suc);
        msgToSend.setType("Insert");
        msgToSend.setPred(pre);
        msgToSend.setValues(values);
        Log.e("Sending request to pred", pre);
        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(pre));
        OutputStream out = socket.getOutputStream();
        DataOutputStream writer = new DataOutputStream(out);
        writer.writeUTF(msgToSend.toString());
        out.close();
        writer.close();
        socket.close();

    }

    private void findSuccandPred(String myPort) throws NoSuchAlgorithmException {
        for(Message msg : msgObjects){
            if(msg.getPort().equals(myPort)){
//                Log.e("Coming inside", "findsuccandpred");
                succ = msg.getSucc();
                pred = msg.getPred();
            }
        }

    }

//    private void genSuccandPred(String myPort) {
//        if(myPort.equals("5554")){
//            succ = "5558";
//            pred = "5556";
//        }else if(myPort.equals("5556")){
//            succ = "5554";
//            pred = "5562";
//        }else if(myPort.equals("5558")){
//            succ = "5560";
//            pred = "5554";
//        }else if(myPort.equals("5560")){
//            succ = "5562";
//            pred = "5558";
//        }else if(myPort.equals("55562")){
//            succ = "5556";
//            pred = "5560";
//        }
//    }


    public static <K, V> void printMap(Map<K, V> map) {
        for (Map.Entry<K, V> entry : map.entrySet()) {
            System.out.println("Key : " + entry.getKey()
                    + " Value : " + Integer.parseInt((String) entry.getValue())/2);
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String sel, String[] selectionArgs,
                        String sortOrder) {

        Cursor c=null;
        SQLiteDatabase db = dbHelper.getReadableDatabase();

        String selection ="";
        String sendPort = "";

        if(sel.contains(":")){
            selection = sel.split(":")[0];
            sendPort = sel.split(":")[1];
        }else{
            selection = sel;
            sendPort = myPort;
        }


        String hashKey = "";
        String hashPort = "";


        try {
            hashKey = genHash(selection);
            hashPort = genHash(myPort);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if(pred.equals("")&&succ.equals("")){
            if(selection.equals("*")){
                c = db.query(dbHelper.TABLE_NAME , null , null , null , null, null, null);
                return c;
            }
            else if(selection.equals("@")){
                c = db.query(dbHelper.TABLE_NAME , null , null , null , null, null, null);
                return c;
            }
            else{
                c = db.query(dbHelper.TABLE_NAME , null , "key =?" , new String[]{selection} , null, null, null);
                return c;
            }

        }
        else if(selection.equals("@")){
            System.out.println("Inside else insert of Query");
            c = db.query(dbHelper.TABLE_NAME , null , null , null , null, null, null);
            return c;
        }
        else if(selection.equals("*")){
            System.out.println("Inside the * of query operation");

            c = db.query(dbHelper.TABLE_NAME , null , null , null , null, null, null);

            System.out.println("Sending port inside * query "+sendPort);
            Message msg = new Message("Query",myPort);
            msg.setSendPort(myPort);
            msg.setPred(pred);
            msg.setSucc(succ);
            msg.setKey(selection);

            try {
                sendQuery(msg);
            } catch (IOException e) {
                e.printStackTrace();
            }

            while(!flag) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            MatrixCursor matrixCursor = new MatrixCursor(new String[] {"key","value"});
            for (String key:starStore.keySet())
            {
                matrixCursor.addRow(new String[] {key,starStore.get(key)});
            }

            if (c.moveToFirst()) {
                do {
                    key = c.getString(c.getColumnIndex("key"));
                    value = c.getString(c.getColumnIndex("value"));
                    matrixCursor.addRow(new String[] {key,value});
                } while (c.moveToNext());
            }

            return matrixCursor;
        }
        else{
            System.out.println("Inside single insert of Query");

            System.out.println("Selection ISK == "+selection);

            if(hashKey.compareTo(hashPort)<0 && hashKey.compareTo(pred)>=0){
                //Own key
                Log.e(TAG,"Query 1");
                c = db.query(dbHelper.TABLE_NAME , null , "key =?" , new String[]{selection} , null, null, null);
                return c;
            }
            else if(hashPort.compareTo(pred)<0 && hashPort.compareTo(succ)<0){
                //First node in ring
                if(hashKey.compareTo(pred)>=0 || hashKey.compareTo(hashPort)<0) {
                    Log.e(TAG,"Query 2");
                    c = db.query(dbHelper.TABLE_NAME , null , "key =?" , new String[]{selection} , null, null, null);
                    return c;
                }
                else{
                    //Send query to successor
                    Log.e(TAG,"Query 3");
                    Message msg = new Message("Query",myPort);
                    msg.setPred(pred);
                    msg.setSucc(succ);
                    msg.setKey(selection);
                    msg.setSendPort(sendPort);

                    try {
                        sendQuery(msg);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    while(outputStore.get(selection) == null)
                    {
                        System.out.println("Entering sleep");
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    Log.e("Creating","matrix cursor");
                    MatrixCursor matrixCursor = new MatrixCursor(new String[] { "key", "value" });
                    matrixCursor.addRow(new String[] {selection, outputStore.get(selection)});
                    return matrixCursor;

                }
            }
            else{
                //Send query to successor
                Log.e(TAG,"Query 4");
                Message msg = new Message("Query",myPort);
                msg.setPred(pred);
                msg.setSucc(succ);
                msg.setKey(selection);
                msg.setSendPort(sendPort);
                try {
                    sendQuery(msg);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                while(outputStore.get(selection) == null)
                {
                    System.out.println("Entering sleep");
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                Log.e("Creating","matrix cursor");
                MatrixCursor matrixCursor = new MatrixCursor(new String[] { "key", "value" });
                matrixCursor.addRow(new String[] {selection, outputStore.get(selection)});
                return matrixCursor;

            }
        }
//        return c;
    }

    private void sendQuery(Message msg) throws IOException {

        String suc = ring.get(msg.getSucc());
        System.out.println("Sending --"+msg.toString()+" -- query to "+suc);
//        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg.toString(),suc);

        System.out.println("requesting socket");
        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(suc));
        System.out.println("socket connect established");

        OutputStream out = socket.getOutputStream();
        DataOutputStream writer = new DataOutputStream(out);
        writer.writeUTF(msg.toString());
        System.out.println("write completed");

        out.close();
        writer.close();
        socket.close();

    }

    private Cursor sendOutput(Message msg) throws IOException {

        String remotePort = msg.getSendPort();
        Log.e("Sending output to ",remotePort);
//        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg.toString(),pre);

        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort)*2);
        OutputStream out = socket.getOutputStream();
        DataOutputStream writer = new DataOutputStream(out);
        writer.writeUTF(msg.toString());
        out.close();
        writer.close();
        socket.close();

        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    @Override
    public boolean onCreate() {

        Log.d(TAG, "main oncreate called");
        dbHelper = new DBHelper(getContext());
        SimpleDhtActivity sda = new SimpleDhtActivity();

        myPort = sda.getPortNo(getContext());

        for(int i =0;i<5;i++) {
            try {
                ring.put(genHash(portList[i]), String.valueOf(Integer.parseInt(portList[i]) * 2));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }

        printMap(ring);

        Log.d("PORT NO:", myPort);

        try {
            hashID = genHash(myPort);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }

        Message message = new Message("Join",myPort);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message.toString(),firstPort);

        Log.d("Hashed PORT NO:", hashID);

        return true;
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... strings) {

            Log.i(TAG,"inside clienttask");

            String msgToSend = strings[0];
            String remotePort = strings[1];

            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
                OutputStream out = socket.getOutputStream();
                DataOutputStream writer = new DataOutputStream(out);
                writer.writeUTF(msgToSend);
                out.close();
                writer.close();
                socket.close();

            } catch (IOException e) {
                e.printStackTrace();
            }


            return null;
        }

    }
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {

            Log.e(TAG, "Server Task called");

            ServerSocket serverSocket = serverSockets[0];

            while(true) {

                try {
                    Log.e(TAG, "Listening for request");
                    Socket socket = serverSocket.accept();
                    Log.e(TAG, "Socket accepted");
                    InputStream in = socket.getInputStream();
                    OutputStream out = socket.getOutputStream();
                    DataInputStream reader = new DataInputStream(in);
                    DataOutputStream writer = new DataOutputStream(out);

                    String reqMsg = reader.readUTF();

                    Message msgObj = new Message();

                    String type = reqMsg.split(":")[0];
                    String port = reqMsg.split(":")[1];

                    msgObj.setType(type);
                    msgObj.setPort(port);

                    if(msgObj.getType().equals("Join")){

                        try {
                            sortedList.put(genHash(msgObj.getPort()), String.valueOf(Integer.parseInt(msgObj.getPort()) * 2));
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }

                        Log.e(TAG,"SortedList:::");
                        printMap(sortedList);

                        int size = sortedList.size();
                        Log.e("Size: ",String.valueOf(size));
                        LinkedList<String> tempList = new LinkedList<String>();

                        if(size>1){
                            for (String key : sortedList.keySet()) {
                                tempList.add(key);
                            }
                            int index = tempList.indexOf(genHash(msgObj.getPort()));

                            Log.e("Index: ",String.valueOf(index));


                            if(index == 0){
                                msgObj.setSucc(tempList.get(1));
                                msgObj.setPred(tempList.get(size-1));
                            }
                            else if(index == size-1){
                                msgObj.setSucc(tempList.get(0));
                                msgObj.setPred(tempList.get(size-2));
                            }
                            else{
                                msgObj.setSucc(tempList.get(index+1));
                                msgObj.setPred(tempList.get(index-1));
                            }

                            Log.e("Inside Servertask", msgObj.toString1());
                            msgObjects.add(msgObj);
                            msgObj.setType("SetSuccandPred");

                            sendSuccandPred(msgObj);

                        }else{
                            temp.setType(msgObj.getType());
                            temp.setPort(msgObj.getPort());
                        }
                        if(msgObjects.size()==4){
                            temp.setSucc(tempList.get(3));
                            temp.setPred(tempList.get(1));
                            msgObjects.add(temp);
                        }

                        checkRelocation(msgObj);

                    }else if(msgObj.getType().equals("SetSuccandPred")){

                        Log.e("ReqMsg ISK", reqMsg);

                        succ = genHash(String.valueOf(Integer.parseInt(ring.get(reqMsg.split(":")[2]))/2));
                        pred = genHash(String.valueOf(Integer.parseInt(ring.get(reqMsg.split(":")[3]))/2));

                        Log.e("My port ISK", myPort);
                        Log.e("My succ ISK",succ);
                        Log.e("My pred ISK",pred);
                    }
                    else if(msgObj.getType().equals("SetNewSuccessor")){
                        succ = genHash(port);
                    }
                    else if(msgObj.getType().equals("SetNewPredecessor")){
                        pred = genHash(port);
                    }
                    else if(msgObj.getType().equals("Insert")){
                        Log.e("Inside the", "insert of servertask");

                        ContentValues values = new ContentValues();
                        String key = reqMsg.split(":")[4];
                        String value = reqMsg.split(":")[5];
                        String suc = reqMsg.split(":")[2];
                        String pre = reqMsg.split(":")[3];

                        Log.e("Myport :",myPort);
                        Log.e("MySucc :",succ);
                        Log.e("MyPred :",pred);

                        values.put("key",key);
                        values.put("value",value);

                        insert(mUri,values);
                    }
                    else if(msgObj.getType().equals("Query")) {
                        String key = "";
                        String value = "";
                        String output = "";
                        String hashPort = genHash(myPort);
                        String firstPort =reqMsg.split(":")[1];
                        Log.e("Inside the", "Query of server task");
                        String sendingPort = reqMsg.split(":")[8];

                        Log.e("Selection in servertask", reqMsg.split(":")[6]);
                        String selection = reqMsg.split(":")[6];
                        String hashKey = genHash(selection);

                        System.out.println("Req Msg = "+reqMsg);

                        if(selection.equals("*")){

                            String outStar ="~";
                            System.out.println("Inside the * query of server task");

                            Message starSend = new Message("Query",myPort);
                            starSend.setSendPort(sendingPort);
                            starSend.setPred(pred);
                            starSend.setSucc(succ);
                            starSend.setKey(selection);


                            System.out.println("Sending port = "+starSend.getSendPort());
                            System.out.println("My Port = "+myPort);
                            if (starSend.getSendPort().equals(myPort)) {
                                System.out.println("Inside equal condition");
                                flag = true;
                            }
                            else {

                                SQLiteDatabase db = dbHelper.getReadableDatabase();

                                Cursor c = db.query(dbHelper.TABLE_NAME, null, null, null, null, null, null);

                                if(c==null){
                                    System.out.println("Cursor is null");
                                    outStar = "~";
                                }
                                while (c.moveToNext()) {
                                    key = c.getString(c.getColumnIndex("key"));
                                    value = c.getString(c.getColumnIndex("value"));
                                    outStar += key + "!" + value + "-";
                                }


                                Message starReply = new Message("StarReply", myPort);
                                starReply.setOutStar(outStar);
                                starReply.setSucc(succ);
                                starReply.setPred(pred);
                                starReply.setKey(selection);
                                starReply.setSendPort(sendingPort);

                                sendOutput(starReply);
                                sendQuery(starSend);

                            }
                        }
                        else {
                            if (hashKey.compareTo(hashPort) < 0 && hashKey.compareTo(pred) >= 0) {
                                //Own key
                                Cursor c = query(mUri, null, selection + ":" + sendingPort, null, null);

                                Log.e("Returned Query in", myPort);

                                if (c.moveToFirst()) {
                                    do {
                                        key = c.getString(c.getColumnIndex("key"));
                                        value = c.getString(c.getColumnIndex("value"));
                                    } while (c.moveToNext());
                                }
                                c.close();

                                output = key + ";" + value;

                                Log.e("Returned Output", output);

                                Message msg = new Message("QueryReply", myPort);
                                msg.setSucc(succ);
                                msg.setPred(pred);
                                msg.setOutput(output);
                                msg.setKey(key);
                                msg.setSendPort(sendingPort);
                                sendOutput(msg);

                            } else if (hashPort.compareTo(pred) < 0 && hashPort.compareTo(succ) < 0) {
                                //First node in ring
                                if (hashKey.compareTo(pred) >= 0 || hashKey.compareTo(hashPort) < 0) {
                                    //own key
                                    Cursor c = query(mUri, null, selection + ":" + sendingPort, null, null);

                                    Log.e("Returned Query in", myPort);

                                    if (c.moveToFirst()) {
                                        do {
                                            key = c.getString(c.getColumnIndex("key"));
                                            value = c.getString(c.getColumnIndex("value"));
                                        } while (c.moveToNext());
                                    }
                                    c.close();

                                    output = key + ";" + value;

                                    Log.e("Returned Output", output);

                                    Message msg = new Message("QueryReply", myPort);
                                    msg.setSucc(succ);
                                    msg.setPred(pred);
                                    msg.setOutput(output);
                                    msg.setKey(key);
                                    msg.setSendPort(sendingPort);
                                    sendOutput(msg);
                                } else {
                                    //Send query to successor
                                    Message msg = new Message("Query", myPort);
                                    msg.setPred(pred);
                                    msg.setSucc(succ);
                                    msg.setKey(selection);
                                    msg.setSendPort(sendingPort);
                                    try {
                                        sendQuery(msg);
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }

                                }
                            } else {
                                //Send query to successor
                                Message msg = new Message("Query", myPort);
                                msg.setPred(pred);
                                msg.setSucc(succ);
                                msg.setKey(selection);
                                msg.setSendPort(sendingPort);
                                try {
                                    sendQuery(msg);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                    else if(msgObj.getType().equals("QueryReply")){
                        Log.e("Inside query reply ", "in server task");

                        Log.e("OutKey",reqMsg.split(":")[7].split(";")[0]);
                        Log.e("OutVal",reqMsg.split(":")[7].split(";")[1]);

                        outKey = reqMsg.split(":")[7].split(";")[0];
                        outVal = reqMsg.split(":")[7].split(";")[1];
                        outputStore.put(outKey,outVal);
                    }
                    else if(msgObj.getType().equals("StarReply")){

                        System.out.println("Inside starrrply of server task");
                        System.out.println("reply from "+reqMsg.split(":")[1]);

                        System.out.println("Reply message : "+reqMsg);

                        String output = reqMsg.split(":")[9];

                        if(!(output.length()==1)) {

                            output = output.substring(1, output.length() - 1);

                            System.out.println("Output:" + output);

                            String[] outputs = output.split("-");

                            for (int i = 0; i < outputs.length; i++) {
                                Log.e("Output in loop ", outputs[i]);
                                String[] outP = outputs[i].split("!");
                                starStore.put(outP[0], outP[1]);
                            }
                        }
                    }
                    else if(msgObj.getType().equals("Delete")){

                        String selection = reqMsg.split(":")[6];
                        SQLiteDatabase db = dbHelper.getWritableDatabase();
                        String sendingPort = reqMsg.split(":")[8];
                        String hashKey = genHash(selection);
                        String hashPort = genHash(myPort);
                        int rowsDeleted;

                        if(selection.equals("*")) {

                            Message msg = new Message("Delete", myPort);
                            msg.setSendPort(sendingPort);
                            msg.setSucc(succ);
                            msg.setPred(pred);
                            msg.setKey(selection);
                            if(!(msg.getSendPort().equals(myPort))) {
                                rowsDeleted = db.delete(dbHelper.TABLE_NAME, null, null);
                                sendDelete(msg);
                            }
                        }
                        else{
                            if (hashKey.compareTo(hashPort) < 0 && hashKey.compareTo(pred) >= 0) {
                                //Own key
                                rowsDeleted = db.delete(dbHelper.TABLE_NAME, "key=?", new String[]{selection});

                            } else if (hashPort.compareTo(pred) < 0 && hashPort.compareTo(succ) < 0) {
                                //First node in ring
                                if (hashKey.compareTo(pred) >= 0 || hashKey.compareTo(hashPort) < 0) {
                                    //own key
                                    rowsDeleted = db.delete(dbHelper.TABLE_NAME, "key=?", new String[]{selection});

                                } else {
                                    //Send query to successor
                                    Message msg = new Message("Delete", myPort);
                                    msg.setSendPort(sendingPort);
                                    msg.setSucc(succ);
                                    msg.setPred(pred);
                                    msg.setKey(selection);
                                    sendDelete(msg);
                                }
                            } else {
                                //Send query to successor
                                Message msg = new Message("Delete", myPort);
                                msg.setSendPort(sendingPort);
                                msg.setSucc(succ);
                                msg.setPred(pred);
                                msg.setKey(selection);
                                sendDelete(msg);
                            }
                        }
                    }
                    else if(msgObj.getType().equals("CheckKey")){

                        System.out.println("Coming inside checkKey in servertask");

                        SQLiteDatabase db = dbHelper.getReadableDatabase();

                        System.out.println("ReqMsg --"+ reqMsg);

                        Cursor c = db.query(dbHelper.TABLE_NAME, null, null, null, null, null, null);

                        HashMap<String, String> temp = new HashMap<String, String>();

                        while (c.moveToNext()) {
                            System.out.println("Cursor is not null");
                            String k = c.getString(c.getColumnIndex("key"));
                            String v = c.getString(c.getColumnIndex("value"));
                            temp.put(k,v);
                        }

                        System.out.println("The size of hashmap in servertask "+temp.size());

                        for (Map.Entry<String, String> entry : temp.entrySet()) {
                            System.out.println("Inside the for loop of check relocation");
                            String hashKey = genHash(entry.getKey());
                            String hashPort = genHash(myPort);
                            if(hashKey.compareTo(hashPort)<0 && hashKey.compareTo(pred)<0) {
                                System.out.println("This is not my Message");
                                int rowsDeleted = db.delete(dbHelper.TABLE_NAME, "key = ?", new String[] {entry.getKey()});

                                ContentValues values = new ContentValues();
                                values.put("key",entry.getKey());
                                values.put("value",entry.getValue());

                                sendRequesttoPred(values,ring.get(succ),ring.get(pred));

                            }
                            else if(hashPort.compareTo(pred)<0){
                                if(hashKey.compareTo(pred)<0 && hashKey.compareTo(hashPort)>0) {

                                    System.out.println("This is not my Message");
                                    int rowsDeleted = db.delete(dbHelper.TABLE_NAME, "key = ?", new String[] {entry.getKey()});

                                    ContentValues values = new ContentValues();
                                    values.put("key",entry.getKey());
                                    values.put("value",entry.getValue());

                                    sendRequesttoPred(values,ring.get(succ),ring.get(pred));

                                }
                            }
                        }
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }

//            return null;
        }

        private void checkRelocation(Message msg) throws NoSuchAlgorithmException, IOException {

            System.out.println("Coming inside check Relocation");

            String suc = ring.get(msg.getSucc());
            String pre = ring.get(msg.getPred());
            System.out.println("The succ is "+ring.get(msg.getSucc()));
            System.out.println("The pred is "+ring.get(msg.getPred()));
            System.out.println("Me is "+msg.getPort());

            Message msgObj = new Message("CheckKey", msg.getPort());
            msgObj.setPred(pre);
            msgObj.setSucc(suc);
            msgObj.setSendPort(myPort);

            System.out.println("Sending relocate msg to "+suc+" == "+msgObj.toString());
            if(suc!=null) {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(suc));
                OutputStream out = socket.getOutputStream();
                DataOutputStream writer = new DataOutputStream(out);
                writer.writeUTF(msgObj.toString());
                out.close();
                writer.close();
                socket.close();
            }

        }

        private void sendRelocationCheck(String s) throws IOException {

            System.out.println("Send Relocation check function");
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(s));

            Message msg = new Message("CheckKey",myPort);
            msg.setSendPort(myPort);
            OutputStream out = socket.getOutputStream();
            DataOutputStream writer = new DataOutputStream(out);
            writer.writeUTF(msg.toString());
            out.close();
            writer.close();
            socket.close();

        }


        private void sendSuccandPred(Message msgObj) throws IOException {

            int succ = Integer.parseInt(ring.get(msgObj.getSucc()));
            int pred = Integer.parseInt(ring.get(msgObj.getPred()));
            Log.e("send succ&pred ISK", msgObj.toString1());

            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgObj.getPort())*2);

            OutputStream out = socket.getOutputStream();
            DataOutputStream writer = new DataOutputStream(out);
            writer.writeUTF(msgObj.toString());
            out.close();
            writer.close();
            socket.close();

            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), succ);
            msgObj.setType("SetNewPredecessor");
            out = socket.getOutputStream();
            writer = new DataOutputStream(out);
            writer.writeUTF(msgObj.toString());
            out.close();
            writer.close();
            socket.close();

            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), pred);
            msgObj.setType("SetNewSuccessor");
            out = socket.getOutputStream();
            writer = new DataOutputStream(out);
            writer.writeUTF(msgObj.toString());
            out.close();
            writer.close();
            socket.close();

        }

    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
}