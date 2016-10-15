package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.ref.SoftReference;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final Uri Content_URI = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
	public int ServerPort = 10000;
	String portStr;
	String myPort;
	String replicaOne;
	String replicaTwo;
	String[] ports = {"5554","5556","5558","5560","5562"};

	TreeMap<String,String> sortedMap = new TreeMap<String, String>();
	String hashPort=null;
	ArrayList<String> portList;
	ArrayList<String> hashList;
	String pred;
	String suc;
	int count=0;
	String pred1;


	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		Context context = getContext();

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr)) * 2);



		try {
			hashPort = genHash(portStr);
			sortedMap.put(genHash("5554"),"5554");
			sortedMap.put(genHash("5556"),"5556");
			sortedMap.put(genHash("5558"),"5558");
			sortedMap.put(genHash("5560"),"5560");
			sortedMap.put(genHash("5562"),"5562");

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		portList =  new ArrayList<String>(sortedMap.values());
		hashList = new ArrayList<String>(sortedMap.keySet());

		for (int i=0; i<portList.size();i++){
			if (portStr.equals(portList.get(i))){
				if (i==0){
					replicaOne=portList.get(1);
					replicaTwo=portList.get(2);
					suc=portList.get(1);
					pred=portList.get(4);
					pred1= portList.get(3);
				}
				else if (i==4){
					replicaOne=portList.get(0);
					replicaTwo=portList.get(1);
					suc=portList.get(0);
					pred=portList.get(3);
					pred1 = portList.get(2);
				}
				else if (i==3){
					replicaOne=portList.get(4);
					replicaTwo=portList.get(0);
					suc=portList.get(4);
					pred=portList.get(2);
					pred1=portList.get(1);
				}
				else if (i==2){
					replicaOne=portList.get(3);
					replicaTwo=portList.get(4);
					suc=portList.get(3);
					pred=portList.get(1);
					pred1=portList.get(0);
				}
				else if (i==1){
					replicaOne=portList.get(2);
					replicaTwo=portList.get(3);
					suc=portList.get(2);
					pred=portList.get(0);
					pred1=portList.get(4);
				}
			}
		}
		Log.i(TAG,"PortNo: "+portStr);
		Log.i(TAG,"Replica One: "+replicaOne);
		Log.i(TAG, "Replica Two: " + replicaTwo);
		Log.i(TAG, "Pred: " +pred);
		Log.i(TAG, "Suc: " + suc);

		try {
			ServerSocket serverSocket = new ServerSocket(ServerPort);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			e.printStackTrace();
		}


		return false;
	}


	public String recon(){
		try{
			//String[] dirNode = getContext().fileList();
			Log.e(TAG,"Reconciling is happening");
			String[] dirSuc=null;
			String[] dirPred=null;
			String sucReconcile = "Reconcile"+"#"+suc;
			String returnMessage1 = "";
			returnMessage1 = reconcile(sucReconcile);
			Log.e(TAG,"REcon message received from suc"+returnMessage1);

			String predReconcile = "Reconcile"+"#"+pred;
			String returnMessage2="";
			returnMessage2 =  reconcile(predReconcile);
			Log.e(TAG,"REcon message received from pred"+returnMessage2);

			ArrayList<String> PredKeyStoreList=null;
			ArrayList<String> PredValueStoreList=null;
			ArrayList<String> SucKeyStoreList=null;
			ArrayList<String>SucValueStoreList=null;

			if (returnMessage1.isEmpty() || returnMessage2.isEmpty()){
				return null;
			}

			if (!returnMessage1.isEmpty() && !returnMessage2.isEmpty()) {
				dirSuc = returnMessage1.split("#");
				dirPred = returnMessage2.split("#");
				//ArrayList<String> NodeList = (ArrayList<String>) Arrays.asList(dirNode);

				String predKey = dirPred[0];
				String predValue = dirPred[1];
				String sucKey = dirSuc[0];
				String sucValue = dirSuc[1];

				String[] predKeyList = predKey.split(",");
				String[] predValueList = predValue.split(",");

				String[] sucKeyList = sucKey.split(",");
				String[] sucValueList = sucValue.split(",");

				PredKeyStoreList = new ArrayList<String>(Arrays.asList(predKeyList));
				PredValueStoreList = new ArrayList<String>(Arrays.asList(predValueList));
				SucKeyStoreList = new ArrayList<String>(Arrays.asList(sucKeyList));
				SucValueStoreList = new ArrayList<String>(Arrays.asList(sucValueList));

				for (int i=0; i<PredKeyStoreList.size();i++){
					String coord = getCoordinator(PredKeyStoreList.get(i));
					if (coord.equals(pred) || coord.equals(pred1)){
						FileOutputStream outputStream;
						outputStream = getContext().openFileOutput(PredKeyStoreList.get(i), Context.MODE_PRIVATE);
						outputStream.write(PredValueStoreList.get(i).getBytes());
						outputStream.close();
					}
				}
				for (int j=0;j<SucKeyStoreList.size();j++){
					String coord = getCoordinator(SucKeyStoreList.get(j));
					if (coord.equals(portStr)){
						FileOutputStream outputStream;
						outputStream = getContext().openFileOutput(SucKeyStoreList.get(j), Context.MODE_PRIVATE);
						outputStream.write(SucValueStoreList.get(j).getBytes());
						outputStream.close();
					}
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}


	/*..............................................................................................*/
	public String reconcile(String msg){
		Log.e(TAG, "it is coming here atleast");
		String[] message = msg.split("#");
		String msgType = message[0];
		Log.i(TAG, "Port Reconcilliation - PortNo : " + message[1]);
		String portNo = message[1];
		String response = "";

		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portNo) * 2);
			OutputStream newStream = socket.getOutputStream();
			DataOutputStream newS = new DataOutputStream(newStream);
			newS.writeUTF(msg);

			InputStream newInStream = socket.getInputStream();
			DataInputStream newInS = new DataInputStream(newInStream);
			response = newInS.readUTF();
			socket.close();
		} catch (IOException e) {
			Log.e(TAG, "Getting stuck here");
			e.printStackTrace();
			return response;
		}

		return response;
	}



	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		String key = values.get("key").toString();
		String value =values.get("value").toString();
		String coordinator = null;
		int count=0;

		try {
			coordinator = getCoordinator(key);
			Log.e(TAG,"Key belongs to: "+coordinator);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}


		/*If coordinator is equal to the actual port - add file into replicas and itself */
		if (coordinator.equals(portStr)){
			FileOutputStream outputStream;
			try {
				outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
				outputStream.write(value.getBytes());
				outputStream.close();
				Log.e(TAG,"Key inserted: "+key);
			} catch (IOException e) {
				e.printStackTrace();
			}
			String forwardInsert = "InsertOne"+"#"+key+"#"+value + "#" + replicaOne+"#"+replicaTwo;
			fwdInsert(forwardInsert);
			Log.i(TAG,"Keys sent to replicas: "+key);
		}
		else {
			String forwardInsert = "InsertToCoord"+"#"+key+"#"+value+"#"+coordinator;
			Log.e(TAG,"Key forwarded to Coordniator: "+key);
			fwdInsertCoord(forwardInsert);
		}
		return null;
	}

	public void fwdInsert(String msg) {
		String[] message = msg.split("#");
		String msgType = message[0];
		String key = message[1];

		if (msgType.equals("InsertOne")) {
			//Log.i(TAG, "File being sent to replicas: " + key);
			String[] ports = {message[3],message[4]};
			String response = "";
			for (String portNo:ports) {

				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portNo) * 2);
					OutputStream newStream = socket.getOutputStream();
					DataOutputStream newS = new DataOutputStream(newStream);
					newS.writeUTF(msg);

					InputStream newInStream = socket.getInputStream();
					DataInputStream newInS = new DataInputStream(newInStream);
					response = newInS.readUTF();
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}


	public void fwdInsertCoord(String msg){
		String[] message = msg.split("#");
		String msgType = message[0];
		String key = message[1];
		String value = message[2];
		if (msgType.equals("InsertToCoord")){
			String portNo= message[3];
			String response="";
			Log.i(TAG, "File being forwarded to Coordinator: " + key);
			Log.i(TAG, "Coordinator: " + portNo);
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portNo) * 2);
				OutputStream newStream = socket.getOutputStream();
				DataOutputStream newS = new DataOutputStream(newStream);
				newS.writeUTF(msg);

				InputStream newInStream = socket.getInputStream();
				DataInputStream newInS = new DataInputStream(newInStream);
				response = newInS.readUTF();
				socket.close();
			} catch (IOException e) {
				if (response.equals("")){
					String rep1="";
					String rep2="";
					for (int i=0;i<portList.size();i++){
						if (portNo.equals(portList.get(i))){
							if (i==3){
								rep1 = portList.get(4);
								rep2 = portList.get(0);
							}
							else if (i==4){
								rep1 = portList.get(0);
								rep2 = portList.get(1);
							}
							else {
								rep1 = portList.get(i+1);
								rep2 = portList.get(i+2);
							}
						}
					}
					String fwdMessage = "InsertOne"+"#"+key+"#"+value+"#"+rep1+"#"+rep2;
					Log.i(TAG,"Key sent to replicas of coord as coord failed"+key);
					fwdInsert(fwdMessage);
				}
				e.printStackTrace();
			}
		}
	}

	/*..............................................................................................*/
	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		MatrixCursor cursor = null;
		if (selection.equals("@")){
			cursor = new MatrixCursor(new String[]{"key", "value"});
			FileInputStream inputStream = null;
			String[] dir = getContext().fileList();
			Log.d(TAG,"Query:Length " + dir.length);
			for (String s:dir){
				Log.d(TAG, "Inside loop query: " + s);
				try {
					//Log.i("Files", s);
					FileInputStream fileInputStream = getContext().openFileInput(s);
					InputStreamReader input = new InputStreamReader(fileInputStream);
					BufferedReader br = new BufferedReader(input);
					StringBuilder sb = new StringBuilder();
					String Line = br.readLine();

					Log.i(TAG,"Files: "+s);
					Log.i(TAG,"Value: "+Line);

					//Reference - http://stackoverflow.com/questions/22264711/matrixcursor-with-non-db-content-provider

					cursor.addRow(new String[]{s, Line});

				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			cursor.moveToFirst();
			Log.e(TAG, "query length for @: " + cursor.getCount());

			return cursor;
		}
		else if (selection.equals("*")){
			cursor = new MatrixCursor(new String[]{"key", "value"});
			FileInputStream inputStream = null;
			String[] dir = getContext().fileList();
			Log.d(TAG, "Query:Length " + dir.length);

			ArrayList<String>keyList = new ArrayList<String>();
			ArrayList<String>valList = new ArrayList<String>();

			for (String s:dir) {
				Log.d(TAG, "Inside loop query: " + s);
				try {
					//Log.i("Files", s);
					FileInputStream fileInputStream = getContext().openFileInput(s);
					InputStreamReader input = new InputStreamReader(fileInputStream);
					BufferedReader br = new BufferedReader(input);
					StringBuilder sb = new StringBuilder();
					String Line = br.readLine();

					keyList.add(s);
					valList.add(Line);

				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			Log.i(TAG,"Send * request to all other nodes");

			for (String s1: ports){
				if (!s1.equals(portStr)){
					String queryMessage = "QueryStar"+"#"+ s1+"#"+portStr;
					String returnMessage = fwdQueryStar(queryMessage);

					if (returnMessage!=null && !returnMessage.isEmpty()) {
						String[] retMessage = returnMessage.split("#");
						String keys = retMessage[0];
						String vals = retMessage[1];

						String[] keysArr = keys.split(",");
						String[] valsArr = vals.split(",");

						for (String s2 : keysArr) {
							keyList.add(s2);
						}
						for (String s3 : valsArr) {
							valList.add(s3);
						}
					}
				}
			}

			for (int i=0;i<keyList.size();i++){
				cursor.addRow(new String[]{keyList.get(i), valList.get(i)});
			}
			cursor.moveToFirst();
			Log.e(TAG, "Cursor Length for *: " + cursor.getCount());
			return cursor;
		}
		else if (!selection.equals("@") || !selection.equals("*")){
			String key = selection;
			String coordinator ="";

			try {
				coordinator = getCoordinator(key);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

			if (coordinator.equals(portStr)){
				Log.i(TAG,"Query Key from Coord=Node: "+key);
				String queryMessage = "QueryReplica"+"#"+ replicaTwo+"#"+portStr+"#"+key;
				String returnMessage = fwdQueryRep(queryMessage);
				Log.i(TAG,"Query Received from Replica2");

				if (returnMessage.equals("")){
					queryMessage = "QueryReplica"+"#"+ replicaOne+"#"+portStr+"#"+key;
					returnMessage =fwdQueryRep(queryMessage);
					Log.i(TAG,"Query Received from Replica1");
				}
				if (returnMessage.equals("")){
					queryMessage = "QueryReplica"+"#"+ portStr+"#"+portStr+"#"+key;
					returnMessage =fwdQueryRep(queryMessage);
					Log.i(TAG,"Query Received from Self");
				}
				String[] retMessage = returnMessage.split("#");
				if (retMessage[0].equals(key)){
					cursor = new MatrixCursor(new String[]{"key", "value"});
					MatrixCursor.RowBuilder builder = cursor.newRow();
					builder.add(retMessage[0]);
					builder.add(retMessage[1]);
					return cursor;
				}
			}
			else if (!coordinator.equals(portStr)){

				String rep1=null;
				String rep2=null;
				for (int i = 0; i < portList.size(); i++) {
					if (coordinator.equals(portList.get(i))) {
						if (i == 3) {
							rep1 = portList.get(4);
							rep2 = portList.get(0);
						} else if (i == 4) {
							rep1 = portList.get(0);
							rep2 = portList.get(1);
						} else {
							rep1 = portList.get(i + 1);
							rep2 = portList.get(i + 2);
						}
					}
				}
				Log.i(TAG,"Query Key from !Coord=Node's replica2: "+key);
				String queryMessage1 = "QueryReplica"+"#"+ rep2+"#"+portStr+"#"+key;
				String returnMessage1 = fwdQueryRep(queryMessage1);
				Log.i(TAG,"Query Received from Coord's Replica2");

				if (returnMessage1.equals("")){
					queryMessage1 = "QueryReplica"+"#"+ rep1+"#"+portStr+"#"+key;
					returnMessage1 =fwdQueryRep(queryMessage1);
					Log.i(TAG,"Query Received from Coord's Replica2");
				}
				if (returnMessage1.equals("")){
					queryMessage1 = "QueryReplica"+"#"+ coordinator+"#"+portStr+"#"+key;
					returnMessage1 =fwdQueryRep(queryMessage1);
					Log.i(TAG,"Query Received from coordinator");
				}
				String[] retMessage1 = returnMessage1.split("#");
				if (retMessage1[0].equals(key)){
					cursor = new MatrixCursor(new String[]{"key", "value"});
					MatrixCursor.RowBuilder builder = cursor.newRow();
					builder.add(retMessage1[0]);
					builder.add(retMessage1[1]);
					return cursor;
				}
			}

		}

		return null;
	}

	public String fwdQueryStar(String msg){
		String[] message = msg.split("#");
		String msgType = message[0];
		String returnMessage = "";

		/*Send * request to other nodes*/
		if (msgType.equals("QueryStar")) {
			String portNo = message[1];
			Log.i(TAG, "* query forwarded to NextPort: " + portNo);
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portNo) * 2);
				OutputStream newStream = socket.getOutputStream();
				DataOutputStream newS = new DataOutputStream(newStream);
				newS.writeUTF(msg);

						/*reading data from port s*/
				InputStream newInStream = socket.getInputStream();
				DataInputStream newIS = new DataInputStream(newInStream);
				returnMessage = newIS.readUTF();

				socket.close();
			} catch (IOException e) {
				e.printStackTrace();
				return returnMessage;
			}
			return returnMessage;
		}
		return null;
	}

	public String fwdQueryRep(String msg){
		String[] message = msg.split("#");
		String msgType = message[0];
		String returnMessage = "";

		/*Send * request to other nodes*/
		if (msgType.equals("QueryReplica")) {
			String portNo = message[1];
			Log.i(TAG, "* query forwarded to replica: " + portNo);
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portNo) * 2);
				OutputStream newStream = socket.getOutputStream();
				DataOutputStream newS = new DataOutputStream(newStream);
				newS.writeUTF(msg);

						/*reading data from port s*/
				InputStream newInStream = socket.getInputStream();
				DataInputStream newIS = new DataInputStream(newInStream);
				returnMessage = newIS.readUTF();

				socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return returnMessage;
		}
		return null;
	}

	public String fwdQueryCoord(String msg){
		String[] message = msg.split("#");
		String msgType = message[0];
		String returnMessage = "";

		/*Send * request to other nodes*/
		if (msgType.equals("QueryCoord")) {
			String portNo = message[1];
			Log.i(TAG, "* query forwarded to Coordinator: " + portNo);
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portNo) * 2);
				OutputStream newStream = socket.getOutputStream();
				DataOutputStream newS = new DataOutputStream(newStream);
				newS.writeUTF(msg);

						/*reading data from port s*/
				InputStream newInStream = socket.getInputStream();
				DataInputStream newIS = new DataInputStream(newInStream);
				returnMessage = newIS.readUTF();

				socket.close();
			} catch (IOException e) {
				if (returnMessage==null || returnMessage.equals("") ) {
					//"QueryReplica"+"#"+ replicaTwo+"#"+portStr+"#"+key;
					String rep1 = "";
					String rep2 = "";
					for (int i = 0; i < portList.size(); i++) {
						if (portNo.equals(portList.get(i))) {
							if (i == 3) {
								rep1 = portList.get(4);
								rep2 = portList.get(0);
							} else if (i == 4) {
								rep1 = portList.get(0);
								rep2 = portList.get(1);
							} else {
								rep1 = portList.get(i + 1);
								rep2 = portList.get(i + 2);
							}
						}
					}
					Log.d(TAG,"Query forwarded to Coord's replica2 as coord is offline"+rep2);
					String fwdQuery = "QueryReplica"+"#"+ rep2+"#"+portStr+"#"+message[3];
					String retMessage = fwdQueryRep(fwdQuery);
					if (retMessage.equals("")){
						Log.d(TAG,"Query forwarded to Coord's replica1 as coord is offline"+rep2);
						fwdQuery = "QueryReplica"+"#"+ rep1+"#"+portStr+"#"+message[3];
						retMessage = fwdQueryRep(fwdQuery);
					}
					returnMessage = retMessage;
				}
				e.printStackTrace();
			}
			return returnMessage;
		}
		return null;
	}


	/*..............................................................................................*/
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		String delKeys="";
		if (selection.equals("@")){
			File directory = getContext().getFilesDir();
			String[] dir = directory.list();
			for (String s : dir) {
				delKeys += s + ",";
				getContext().deleteFile(s);
			}
			String delAt = "DelAt" + "#" + delKeys + "#" + replicaOne + "#" + replicaTwo;
			fwdDelete(delAt);
		}
		else if (selection.equals("*")){
			File directory = getContext().getFilesDir();
			String[] dir = directory.list();
			for (String s : dir) {
				getContext().deleteFile(s);
			}
			String delAll = "DelAll"+"#"+portStr;
			fwdDelete(delAll);
		}
		else if (!selection.equals("@") || !selection.equals("*")){
			String key = selection;
			String coordinator=null;

			try {
				coordinator= getCoordinator(key);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

			/*if (coordinator.equals(portStr)){
				getContext().deleteFile(key);
				String delSingle = "DelSingle"+"#"+key+"#"+replicaOne+"#"+replicaTwo;
				fwdDelete(delSingle);
			}
			else{
				String delFromCoord = "DelFromCoord"+"#"+key+"#"+coordinator;
				fwdDelete(delFromCoord);
			}*/
			for (int i=0;i<portList.size();i++){
				String delSingleFromAll = "DelSingleFromAll"+"#"+key+"#"+portList.get(i);
				fwdDelete(delSingleFromAll);
			}

		}
		return 0;
	}

	public void fwdDelete(String msg){
		String[] message = msg.split("#");
		String msgType = message[0];
		String response="";

		if (msgType.equals("DelAt") || msgType.equals("DelSingle")){
			String[] recPorts = {message[2],message[3]};
			for (String portNo:recPorts){
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portNo) * 2);
					OutputStream newStream = socket.getOutputStream();
					DataOutputStream newS = new DataOutputStream(newStream);
					newS.writeUTF(msg);

					InputStream newInStream = socket.getInputStream();
					DataInputStream newInS = new DataInputStream(newInStream);
					response = newInS.readUTF();
					socket.close();

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		else if (msgType.equals("DelAll")){
			for (String portNo:ports){
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portNo) * 2);
					OutputStream newStream = socket.getOutputStream();
					DataOutputStream newS = new DataOutputStream(newStream);
					newS.writeUTF(msg);

					InputStream newInStream = socket.getInputStream();
					DataInputStream newInS = new DataInputStream(newInStream);
					response = newInS.readUTF();
					socket.close();

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		else if (msgType.equals("DelFromCoord")){
			String portNo= message[2];
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portNo) * 2);
				OutputStream newStream = socket.getOutputStream();
				DataOutputStream newS = new DataOutputStream(newStream);
				newS.writeUTF(msg);

				InputStream newInStream = socket.getInputStream();
				DataInputStream newInS = new DataInputStream(newInStream);
				response = newInS.readUTF();
				socket.close();

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else if (msgType.equals("DelSingleFromAll")){
			String portNo = message[2];
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portNo) * 2);
				OutputStream newStream = socket.getOutputStream();
				DataOutputStream newS = new DataOutputStream(newStream);
				newS.writeUTF(msg);

				InputStream newInStream = socket.getInputStream();
				DataInputStream newInS = new DataInputStream(newInStream);
				response = newInS.readUTF();
				socket.close();

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/*..............................................................................................*/
	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
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

	/*Returns the Coordinator for the given key*/
	public String getCoordinator(String key) throws NoSuchAlgorithmException {
		String keyHash = genHash(key);
		String coordinator = null;
		for (int j=0;j<portList.size();j++){
			if (j!=0) {
				if ((keyHash.compareTo(hashList.get(j)) <= 0 && keyHash.compareTo(hashList.get(j - 1)) > 0) ||
						(keyHash.compareTo(hashList.get(j-1)) < 0 && keyHash.compareTo(hashList.get(j)) <= 0 && hashList.get(j-1).compareTo(hashList.get(j)) > 0) ||
						(keyHash.compareTo(hashList.get(j-1)) > 0 && keyHash.compareTo(hashList.get(j)) > 0 && hashList.get(j-1).compareTo(hashList.get(j)) > 0)) {
					coordinator = portList.get(j);
				}
			}
			else if (j==0){
				if ((keyHash.compareTo(hashList.get(0)) <=0 && keyHash.compareTo(hashList.get(4)) >0) ||
						(keyHash.compareTo(hashList.get(4)) <0 && keyHash.compareTo(hashList.get(0)) <=0 && hashList.get(4).compareTo(hashList.get(0)) >0) ||
						(keyHash.compareTo(hashList.get(4)) >0 && keyHash.compareTo(hashList.get(0)) >0 && hashList.get(4).compareTo(hashList.get(0)) >0)){
					coordinator = portList.get(0);
				}
			}
		}
		return coordinator;
	}


	public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			recon();

			ServerSocket serverSocket = sockets[0];
			try {
				while (true) {
					Socket mySocket = serverSocket.accept();
					Thread temp = new Thread(new TempThread(mySocket));
					temp.start();
				}

			} catch (IOException e) {
				e.printStackTrace();
			}

			return null;
		}
	}

	public class TempThread implements Runnable{
		Socket mySocket;

		public TempThread(Socket socket){
			mySocket = socket;
		}

		@Override
		public void run() {
			try {
				DataInputStream newStream = new DataInputStream(mySocket.getInputStream());
				String msg = newStream.readUTF();

				DataOutputStream outputStream = new DataOutputStream((mySocket.getOutputStream()));

				String[] message = msg.split("#");
				String msgType = message[0];

					/*Receive insert message from Coordinator*/
				if (msgType.equals("InsertOne")) {
					String key = message[1];
					String value = message[2];

					FileOutputStream outputStream1;
					try {
						outputStream1 = getContext().openFileOutput(key, Context.MODE_PRIVATE);
						outputStream1.write(value.getBytes());
						outputStream1.close();
						Log.e(TAG, "Inserted key into replicas: " + key);
					} catch (IOException e) {
						e.printStackTrace();
					}
					outputStream.writeUTF("abc");

				}
					/*Recive message from other node to coordinator*/
				else if (msgType.equals("InsertToCoord")) {
					String key = message[1];
					String value = message[2];
					ContentValues cv = new ContentValues();
					cv.put("key", key);
					cv.put("value", value);
					insert(Content_URI, cv);
					outputStream.writeUTF("abc");
				}
					/*Delete @-selection from replicas*/
				else if (msgType.equals("DelAt")) {
					String keyList = message[1];
					String[] dir = keyList.split(",");
					for (String s : dir) {
						getContext().deleteFile(s);
						Log.i(TAG, "Deleted file from replica: " + s);
					}
					outputStream.writeUTF("done");
				}
					/*Delete *-selection from all Nodes*/
				else if (msgType.equals("DelAll")) {
					String portNo = message[1];
					if (!portNo.equals(portStr)) {
						File directory = getContext().getFilesDir();
						String[] dir = directory.list();
						for (String s : dir) {
							getContext().deleteFile(s);
						}
						Log.i(TAG, "All files deleted from Node: " + portStr);
					} else {
						Log.i(TAG, "All files deleted from Node: " + portStr);
					}
					outputStream.writeUTF("done");
				}
					/*Delete Single File-selecton from replicas*/
				else if (msgType.equals("DelSingle")) {
					String key = message[1];
					getContext().deleteFile(key);
					Log.i(TAG, "Deleted file from replica: " + key);
					outputStream.writeUTF("done");
				}
				else if (msgType.equals("DelSingleFromAll")) {
					String key = message[1];
					getContext().deleteFile(key);
					Log.i(TAG, "Deleted file from Node: " + key);
					outputStream.writeUTF("done");
				}
					/*Delete forwarded to Coordinator*/
				else if (msgType.equals("DelFromCoord")) {
					String key = message[1];
					String portNo = message[2];
					delete(Content_URI, key, null);
					Log.i(TAG, "Delete file received at Coordinator: " + portNo);
					outputStream.writeUTF("done");
				}
				//Query *-selection from all Nodes
				else if (msgType.equals("QueryStar")) {
					String returnMessage = message[1];

					String[] dir = getContext().fileList();
					Log.d(TAG, "Query:Length " + dir.length);

					String keyArr = "";
					String valArr = "";

					for (String s : dir) {
						Log.d(TAG, "Inside loop query: " + s);
						try {
							//Log.i("Files", s);
							FileInputStream fileInputStream = getContext().openFileInput(s);
							InputStreamReader input = new InputStreamReader(fileInputStream);
							BufferedReader br = new BufferedReader(input);
							StringBuilder sb = new StringBuilder();
							String Line = br.readLine();

							keyArr += s + ",";
							valArr += Line + ",";

						} catch (FileNotFoundException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					String returnToMain = keyArr + "#" + valArr;
					outputStream.writeUTF(returnToMain);
				}
				else if (msgType.equals("QueryReplica")){
					String key = message[3];
					try {
						FileInputStream inputStream = getContext().openFileInput(key);
						InputStreamReader input = new InputStreamReader(inputStream);
						BufferedReader br = new BufferedReader(input);
						StringBuilder sb = new StringBuilder();
						String Line = br.readLine();
						String returnMessage = key+"#"+Line;
						Log.i(TAG,"Key Value sent back to coord"+Line);
						outputStream.writeUTF(returnMessage);

					} catch (FileNotFoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				else if (msgType.equals("QueryCoord")){
					String key = message[3];
					String returnMainPort = message[2];

					String queryMessage = "QueryReplica"+"#"+replicaTwo+"#"+portStr+"#"+key;
					String returnMessage = fwdQueryRep(queryMessage);
					Log.i(TAG,"Query Received from Coord's Replica");
					String[] retMessage = returnMessage.split("#");
					if (retMessage[0].equals(key)){
						String retToMain = retMessage[0]+"#"+retMessage[1];
						Log.i(TAG,"Key-Value sent from Coord to Main Node");
						outputStream.writeUTF(retToMain);
					}
				}
				else if (msgType.equals("Reconcile")) {
					Log.i(TAG, "Get file Index to the returned port");
					String[] dir = getContext().fileList();
					Log.d(TAG, "Query:Length " + dir.length);

					String keyArr = "";
					String valArr = "";

					for (String s : dir) {
						Log.d(TAG, "Inside loop query: " + s);
						try {
							//Log.i("Files", s);
							FileInputStream fileInputStream = getContext().openFileInput(s);
							InputStreamReader input = new InputStreamReader(fileInputStream);
							BufferedReader br = new BufferedReader(input);
							StringBuilder sb = new StringBuilder();
							String Line = br.readLine();

							keyArr += s + ",";
							valArr += Line + ",";

						} catch (FileNotFoundException e) {
							outputStream.writeUTF("");
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}

					if(keyArr!=null && valArr!=null && !keyArr.isEmpty() && !valArr.isEmpty()){
						String returnToMain = keyArr + "#" + valArr;
						outputStream.writeUTF(returnToMain);}
					else {
						outputStream.writeUTF("");
					}
				}


			}catch (IOException e) {
				e.printStackTrace();
			}
		}


	}

}

