package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

	public static String myPort=null;
	static final int SERVER_PORT = 10000;
	public static String[] REMOTE_PORTS={"11108","11112","11116","11120", "11124"};
	static final int MAIN_PORT = 11108;
	public static String successor = "0";
	public static String predec = "0";
	public static String myHash = null;
	private SQLiteDatabase database;
	MyDBHelper DBHelper;
	public static final String AUTHORITY ="edu.buffalo.cse.cse486586.simpledht.provider";
	public static final Uri CONTENT_URI = Uri.parse("content://" + AUTHORITY);
	public static final String table_name= "OBJECTS";
	private int firstJoin = 0;
	private HashMap<String,String> map=new HashMap<String,String>();
	private boolean secondStage = false;
	String TAG = "MY DEBUGGER";
	String TAG2 = "STAR!";
	
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		String where = "key=?";
		
		return database.delete(table_name, where, selection == "*" ? null : new String[] {selection});
		
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues valuesToInsert) {
	
		String key = valuesToInsert.getAsString("key");
		String value = valuesToInsert.getAsString("value");
		try{
		if((successor.equals("0")&&predec.equals("0"))){
		//	Log.d("ENTERED","stage one insert!");
			SQLiteDatabase database = DBHelper.getWritableDatabase();
			long val = database.insert(table_name, null, valuesToInsert);
		//	Log.d("insert-stage1", valuesToInsert.toString());
			return Uri.withAppendedPath(CONTENT_URI, String.valueOf(value));
		}
		else{
			String hash = genHash(key);
			if(genHash(predec).compareTo(myHash)<0){ //predec less than me
			
				if ((hash.compareTo(myHash)<0)&&(hash.compareTo(genHash(predec))>0)){
				//	SQLiteDatabase database = DBHelper.getWritableDatabase();
					long val = database.insert(table_name, null, valuesToInsert);
		//			Log.v("insert-stage1", valuesToInsert.toString());
					return Uri.withAppendedPath(CONTENT_URI, String.valueOf(value));
				}else{
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "FORWARD", map.get(successor), key, value);
				}
			}else{ //predec greater than me
				if((hash.compareTo(genHash(predec))>0)||(hash.compareTo(myHash)<0)){
					//SQLiteDatabase database = DBHelper.getWritableDatabase();
					long val = database.insert(table_name, null, valuesToInsert);
		//			Log.v("insert-stage1", valuesToInsert.toString());
					return Uri.withAppendedPath(CONTENT_URI, String.valueOf(value));
				}else{
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "FORWARD", map.get(successor), key, value);
				}
			}
			
		}
		}catch(Exception e){
			Log.e("SHA error",e.toString());
		}
		return Uri.withAppendedPath(CONTENT_URI, String.valueOf(value));
		
	}

	@Override
	public boolean onCreate() {
		DBHelper = new MyDBHelper(getContext());
		database = DBHelper.getWritableDatabase();
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		
		map.put("11108","5554" );
		map.put("11112","5556" );
		map.put("11116","5558" );
		map.put("11120","5560" );
		map.put("11124","5562" );
		map.put("5562","11124" );
		map.put("5560","11120" );
		map.put("5558","11116" );
		map.put("5556","11112" );
		map.put("5554","11108" );

		try {
			myHash = genHash(map.get(myPort));
		} catch (NoSuchAlgorithmException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
	
		String joinMsg = myPort;
		if(!myPort.equals("11108"))
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, joinMsg, myPort);
		
		try{
			
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);

			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e("Error", "Can't create a ServerSocket");
		}
		
		return true;
	}
	private String Serialized(Cursor cu){
		String key = null;
		String value = null;
		String result = "";
		
		
		
		if (cu.moveToFirst()){
			while(!cu.isAfterLast()){
				key = cu.getString(cu.getColumnIndex("key"));
				value = cu.getString(cu.getColumnIndex("value"));
				if(result.equals(""))
				result = key + "|" + value;
				else
				result = result + " " + key + "|" + value;
				cu.moveToNext();
			}
		}
		
		if(result.equals("")){
			Log.d(TAG2,"result is empty in query");
		}
		
		return result;
	}
	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
			String sortOrder) {
		
		Log.v("SELECTION", selection);
		//Log.v("Successor", successor);
		//Log.v("Predecessor", predec);
		Cursor cursor=null;
		
		if(successor.equals("0")&&predec.equals("0")){
		
		if(selection.equals("*")||selection.equals("@")){
			cursor = database.rawQuery("SELECT * FROM "+table_name,null);
		}
		else{
		String query_statement= "SELECT * FROM "+table_name+" WHERE key=?";
		String[] sel=new String[1];
		sel[0]=selection;
		cursor = database.rawQuery(query_statement,sel);
		}
		}else{
			if(selection.equals("@")){
				cursor = database.rawQuery("SELECT * FROM "+table_name,null);
			}else if(selection.equals("*")){
				//new code
				cursor = database.rawQuery("SELECT * FROM "+table_name,null);
				
				if(cursor.getCount()==0){
					Log.d(TAG2,"PANIC in query-- cursor row count is 0");
				}
				
				
				String myObject;
				
				myObject = Serialized(cursor);
				
				
				String msg = map.get(myPort)+"!"+selection+"\n";
				String m=msg;
				try{
					Log.d(TAG2, "INITIATOR Port "+map.get(myPort)+" forwarding * request "+ m +" (inside query) "+selection+" to "+successor);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(map.get(successor)));
					PrintWriter printwriter = new PrintWriter(socket.getOutputStream(),true);
					printwriter.write(msg);  //write the message to output stream
					printwriter.flush();
					BufferedReader br= new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String line = br.readLine();
					Log.d(TAG2, "INITIATOR Port "+map.get(myPort)+" received * response "+ m +" (inside query) for "+selection+" rsponse is "+line);
					printwriter.close(); //close the output stream
					br.close();
					socket.close(); //close connection
					if(myObject.isEmpty()&&line==null){
						String[] colNames = {"key", "value"};
						MatrixCursor cu = new MatrixCursor(colNames);
						return cu;
					}
					if(line==null&&!myObject.isEmpty()){
						line = myObject;
					}
					if(line!=null&&!myObject.isEmpty()){
						line = line + " " + myObject;
					}
					if(myObject.isEmpty()&&line!=null){
						
					}
					String[] a = line.split("\\s");
					String[] colNames = {"key", "value"};
					MatrixCursor cu = new MatrixCursor(colNames);
					
					for(int i=0;i<a.length;i++){
						String[] temp = a[i].trim().split("\\|");
						cu.addRow(new String[]{temp[0],temp[1]});
					}
					
					//cu.moveToFirst();
					return cu;
				}catch(Exception e){
					Log.e("ERROR IN QUERY", e.toString());
				}

				//new code ends
			}else{
				String query_statement= "SELECT * FROM "+table_name+" WHERE key=?";
				String[] sel=new String[1];
				sel[0]=selection;
				cursor = database.rawQuery(query_statement,sel);
				Log.d(TAG, "cursor count "+cursor.getCount());
				if(cursor.getCount()==0){
				//forward request
				try{
					Log.d(TAG, "INITIATOR Port "+map.get(myPort)+" forwarding query request(inside query) "+selection+" to "+successor);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(map.get(successor)));
					PrintWriter printwriter = new PrintWriter(socket.getOutputStream(),true);
					printwriter.write("QUERY"+selection+"\n");  //write the message to output stream
					printwriter.flush();
					BufferedReader br= new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String line = br.readLine();
					Log.d(TAG, "INITIATOR Port "+map.get(myPort)+" received response(inside query) for "+selection+" rsponse is "+line);
					printwriter.close(); //close the output stream
					br.close();
					socket.close(); //close connection
					String a[]=line.split("\\|");
					String key = a[0].trim();
					String value = a[1].trim();
					String[] colNames = {"key", "value"};
					MatrixCursor cu = new MatrixCursor(colNames);
					
					Log.d(TAG, "colnames = "+colNames[0]+" "+colNames[1]);
					cu.addRow(new String[]{key, value});
					//cu.moveToFirst();
					return cu;
				}catch(Exception e){
					Log.e("ERROR IN QUERY", e.toString());
				}
			}
			}
				
		}
		cursor.moveToFirst();
		Log.v("query", selection); 
		return cursor;
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

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			/*
			 * TODO: Fill in your server code that receives messages and passes them
			 * to onProgressUpdate().
			 */
		//	Log.d(TAG, "SERVER TASK STARTED");
			ServerSocket serverSocket = sockets[0];
			Socket clientSocket;
			InputStreamReader inputStreamReader;
			BufferedReader bufferedReader;
			String msg=null;
			String hash = null;
			while (true) {
				try {
					clientSocket = serverSocket.accept();   //accept the client connection
					inputStreamReader = new InputStreamReader(clientSocket.getInputStream());
					bufferedReader = new BufferedReader(inputStreamReader); //get the client message
					msg = bufferedReader.readLine();
					//Log.d("RECEIVED", msg + " AT " + myPort);
					secondStage = true;
					
					//stage 3 code:
					if(msg.contains("!")){
						Cursor cursor = null;
						String temp[] = msg.split("\\!");
						cursor = database.rawQuery("SELECT * FROM "+table_name,null);
						
						if(cursor.getCount()==0){
							Log.d(TAG2,"PANIC -- cursor row count is 0");
						}
						
						String myObject;
					
						myObject = Serialized(cursor);
						
						String m = temp[0].trim()+"!"+temp[1].trim()+"\n";
						Log.d(TAG2, "my own cursor is "+myObject);
						if(!successor.equals(temp[0].trim())){
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(map.get(successor)));
							PrintWriter printwriter = new PrintWriter(socket.getOutputStream(),true);
							printwriter.write(temp[0].trim()+"!"+temp[1].trim()+"\n");  //write the message to output stream
							printwriter.flush();
							Log.d(TAG2, "Port "+map.get(myPort)+" forwarding * request "+ m +" (inside client) to "+successor);
							BufferedReader br= new BufferedReader(new InputStreamReader(socket.getInputStream()));
							String line = br.readLine();
							if(myObject.isEmpty()&&line==null){
								line = "";
							}
							if(line==null&&!myObject.isEmpty()){
								line = myObject;
							}
							if(line!=null&&!myObject.isEmpty()){
								line = line + " " + myObject;
							}
							if(myObject.isEmpty()&&line!=null){
								
							}
							PrintWriter wr = new PrintWriter(clientSocket.getOutputStream(), true);
							Log.d(TAG2,"Port "+map.get(myPort)+" responded * request "+ m +" (inside serverTask) with "+line);
							wr.write(line);  //write the message to output stream
							wr.flush();
							wr.close();
						}else{
							PrintWriter wr = new PrintWriter(clientSocket.getOutputStream(), true);
							Log.d(TAG2,"Port "+map.get(myPort)+" has successor as initiator, so responded to query "+m+" (inside serverTask) with "+myObject);
							wr.write(myObject);  //write the message to output stream
							wr.flush();
							wr.close();
						}
					}
					
					
					//stage 2 query code:
					if(msg.contains("QUERY")){
						
						String selection = msg.replace("QUERY", "");
						Log.d(TAG, "Port "+map.get(myPort)+" received the forwarded query request(inside serverTask) for"+selection);
						//check in its own DB:
						  Cursor cursor = null;	
							if(selection.equals("@")){
								cursor = database.rawQuery("SELECT * FROM "+table_name,null);
							}else if(selection.equals("*")){
								
								Log.d(TAG2, "NOT HANDLED");
								
							}else{
								
								String query_statement= "SELECT * FROM "+table_name+" WHERE key=?";
								String[] sel=new String[1];
								sel[0]=selection;
								cursor = database.rawQuery(query_statement,sel);
								Log.d(TAG, "cursor count "+cursor.getCount());
								if(cursor.getCount()==0){
								//forward request
								try{
									Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(map.get(successor)));
									PrintWriter printwriter = new PrintWriter(socket.getOutputStream(),true);
									printwriter.write("QUERY"+selection+"\n");  //write the message to output stream
									printwriter.flush();
									Log.d(TAG, "Port "+map.get(myPort)+" forwarding query request(inside client) "+selection+" to "+successor);
									BufferedReader br= new BufferedReader(new InputStreamReader(socket.getInputStream()));
									String line = br.readLine();
									
									
									PrintWriter wr = new PrintWriter(clientSocket.getOutputStream(), true);
									Log.d(TAG,"Port "+map.get(myPort)+" responded query request(inside serverTask) "+selection+" response is "+line);
									wr.write(line);  //write the message to output stream
									wr.flush();
									wr.close();
									
									
									Log.d(TAG, "Port "+map.get(myPort)+" received response(inside client socket) for "+selection+" rsponse is "+line);
									printwriter.close(); //close the output stream
									br.close();
									socket.close(); //close connection
																	
						}catch(Exception e){
							Log.e("client socket error!", e.toString());
						}
					}else{
						String object = Serialize(cursor);
						PrintWriter wr = new PrintWriter(clientSocket.getOutputStream(), true);
						Log.d(TAG,"Port "+map.get(myPort)+" responded query request(inside serverTask) "+selection+" response is "+object);
						wr.write(object);  //write the message to output stream
						wr.flush();
						wr.close();
					}
						}	
							}
						
						
					
					if(map.containsKey(msg)){
					hash = genHash(map.get(msg));
					}
					inputStreamReader.close(); 
					clientSocket.close();
					
				}catch(Exception e){
					Log.e("SERVERTASK error", e.toString()+e.getMessage());
					e.printStackTrace();
				}
					
				if(myPort.equals("11108")&&firstJoin==0){ 
					firstJoin = 1;
					successor = map.get(myPort);
					predec = map.get(myPort);
				}
				
				
				if(firstJoin == 1){
					reMap(msg, "SUCCESSOR:"+myPort+" AND PREDEC:"+myPort);
					successor = map.get(msg);
					predec =map.get(msg);
					firstJoin++;
					continue;
				}
				
				try{
				//noed join code:
					if(!msg.contains(":")&&!msg.contains("####")&&!msg.contains("QUERY")&&!msg.contains("!")){
						Log.d("PANIC", "should not be here when msg is "+msg);
					if(genHash(predec).compareTo(myHash)<0){ //predec less than me
						
						if ((hash.compareTo(myHash)<0)&&(hash.compareTo(genHash(predec))>0)){
							reMap(map.get(predec),"SUCCESSOR:"+msg);
							reMap(msg,"SUCCESSOR:"+myPort+" AND PREDEC:"+map.get(predec));
							predec = map.get(msg);
						}else{
							reMap(map.get(successor), msg);
						}
					}else{ //predec greater than me
						if((hash.compareTo(genHash(predec))>0)||(hash.compareTo(myHash)<0)){
							reMap(map.get(predec),"SUCCESSOR:"+msg);
							reMap(msg,"SUCCESSOR:"+myPort+" AND PREDEC:"+map.get(predec));
							predec = map.get(msg);
						}else{
							reMap(map.get(successor), msg);
						}
					}
					}
					if(msg.contains(":")){
						if(msg.contains("AND")){
						String a[]=msg.split("AND");
						if(a[0].contains("SUCCESSOR:")){
							successor = map.get((a[0].trim().replace("SUCCESSOR:", "")).trim());
							predec = map.get((a[1].trim().replace("PREDEC:", "")).trim());
						}else{
							successor = map.get((a[1].trim().replace("SUCCESSOR:", "")).trim());
							predec = map.get((a[0].trim().replace("PREDEC:", "")).trim());
						}
						}else{
						if(msg.contains("SUCCESSOR:")&&!msg.contains("PREDEC:")){
							successor = map.get(msg.replace("SUCCESSOR:", "").trim());
						}
						if(!msg.contains("SUCCESSOR:")&&msg.contains("PREDEC:")){
							predec = map.get(msg.replace("PREDEC:", "").trim());
						}
						}
					}
					
					
				
				// Stage 2 insert code:
				if(msg.contains("###")){
					String a[] = msg.split("####");
					String key = a[0];
					String value = a[1];
					ContentValues cv = new ContentValues();
					cv.put("key", a[0]);
					cv.put("value", a[1]);
					Uri mUri = buildUri("content", AUTHORITY);
			//		Log.d("URI", mUri.toString());
					ContentResolver cr = getContext().getContentResolver();
					cr.insert(mUri, cv);
				}
				}catch(Exception e){
					Log.e("SHA error", e.toString());
				}
				
		
			}

		}
		private String Serialized(Cursor cu){
			String key = null;
			String value = null;
			String result = "";
			
			
			if (cu.moveToFirst()){
				while(!cu.isAfterLast()){
					key = cu.getString(cu.getColumnIndex("key"));
					value = cu.getString(cu.getColumnIndex("value"));
					if(result.equals(""))
					result = key + "|" + value;
					else
					result = result + " " + key + "|" + value;
					cu.moveToNext();
				}
			}
			if(result.equals("")){
				Log.d(TAG2,"result is empty");
			}
			
			return result;
		}
		private String Serialize(Cursor cu){
			String key = null;
			String value = null;
			if (cu.moveToFirst()){
				while(!cu.isAfterLast()){
					key = cu.getString(cu.getColumnIndex("key"));
					value = cu.getString(cu.getColumnIndex("value"));
					cu.moveToNext();
				}
			}
			cu.close();
			if(key==null||value==null){
				Log.d(TAG,"Result is null");
			}
			String result = key+"|"+value;
			
			return result;
		}
		private void reMap(String...msgs){
			try{//msg[0]-->destination
				//msg[1]--->msg
				//Log.d("--->SENT",msgs[1]+"from "+myPort+" to "+msgs[0]);
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[0]));
				PrintWriter printwriter = new PrintWriter(socket.getOutputStream(),true);
				printwriter.write(msgs[1]);  //write the message to output stream
				printwriter.flush();
				printwriter.close(); //close the output stream
				socket.close(); //close connection
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		
	
	}


	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			
			if(msgs[0].contains("FORWARD")){
				try{
					String msg=msgs[2]+"####"+msgs[3];
				//	Log.d("MY PORT", myPort);
				//	Log.d("FORWARDING REQUEST FROM", "FROM "+myPort+" "+msg + " TO " + map.get(msgs[1]));
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[1]));
					PrintWriter printwriter = new PrintWriter(socket.getOutputStream(),true);
					printwriter.write(msg);  //write the message to output stream
					printwriter.flush();
					printwriter.close(); //close the output stream
					socket.close(); //close connection
				}catch(Exception e){
					e.printStackTrace();
				}
				
			}else{
			if(myPort!="11108"&& !msgs[0].contains(":")){
				String msg = msgs[0];
				try{
				//	Log.d("MY PORT", myPort);
				//	Log.d("SENDING JOIN REQUEST FROM ", "FROM "+myPort+" "+msg + " TO " + MAIN_PORT);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), MAIN_PORT);
					PrintWriter printwriter = new PrintWriter(socket.getOutputStream(),true);
					printwriter.write(msg);  //write the message to output stream
					printwriter.flush();
					printwriter.close(); //close the output stream
					socket.close(); //close connection
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			}
			return null;

		}
	}
	private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

}
