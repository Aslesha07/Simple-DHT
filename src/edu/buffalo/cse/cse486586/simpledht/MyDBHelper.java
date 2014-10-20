package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

public class MyDBHelper extends SQLiteOpenHelper{


		public static final String MESSAGE_tableName = "OBJECTS";
	    public static final String key = "key";
	    public static final String value = "value";
	 
	    private static final String DATABASE_NAME = "Objects.db";
	    private static final int DATABASE_VERSION = 1;
	 
	    
	    private static final String DATABASE_CREATE = "CREATE TABLE " + MESSAGE_tableName
	            + "(" + key + " TEXT PRIMARY KEY, "
	            + value + " TEXT);";
	    
	    public MyDBHelper(Context context) {
	        super(context, DATABASE_NAME, null, DATABASE_VERSION);
	    }

		@Override
		public void onCreate(SQLiteDatabase db) {
			db.execSQL(DATABASE_CREATE);
		}

		@Override
		public void onUpgrade(SQLiteDatabase db, int oldV, int newV) {
			Log.w(MyDBHelper.class.getName(),
			        "Upgrading database from version " + oldV + " to "
			            + newV + ", which will destroy all old data");
			    db.execSQL("DROP TABLE IF EXISTS " + MESSAGE_tableName);
			    onCreate(db);
		}	 
}
