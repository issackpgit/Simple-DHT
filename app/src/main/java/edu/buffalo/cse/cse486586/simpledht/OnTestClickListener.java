package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.TextView;

public class OnTestClickListener implements OnClickListener {

	private static final String TAG = OnTestClickListener.class.getName();
	private static final int TEST_CNT = 50;
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";

	private final TextView mTextView;
	private final ContentResolver mContentResolver;
	private final Uri mUri;
	private final ContentValues[] mContentValues;

	public OnTestClickListener(TextView _tv, ContentResolver _cr) {
		mTextView = _tv;
		mContentResolver = _cr;
		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
		mContentValues = initTestValues();
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	private ContentValues[] initTestValues() {
		ContentValues[] cv = new ContentValues[TEST_CNT];
		for (int i = 0; i < TEST_CNT; i++) {
			cv[i] = new ContentValues();
			cv[i].put(KEY_FIELD, "key" + Integer.toString(i));
			cv[i].put(VALUE_FIELD, "val" + Integer.toString(i));
		}

		return cv;
	}
// Test
	@Override
	public void onClick(View v) {

		switch(v.getId()){
			case R.id.button3:
				new Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
				break;

			case R.id.button1:
				mTextView.append("LDump pressed\n");
				new LDump().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
				break;

			case R.id.button2:
				mTextView.append("GDump pressed\n");
				new GDump().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
				break;

			case R.id.button4:
				mTextView.setText("");
				break;
		}

	}

	private class LDump extends AsyncTask<Void, String, Void>{

		@Override
		protected Void doInBackground(Void... voids) {

			Cursor resultCursor = mContentResolver.query(mUri, null, "@", null, null);

			while (resultCursor.moveToNext()) {
				String key = resultCursor.getString(resultCursor.getColumnIndex("key"));
				String value = resultCursor.getString(resultCursor.getColumnIndex("value"));
				publishProgress(key + ":" + value +"\n");
			}
			return null;
		}

		protected void onProgressUpdate(String...strings) {
			mTextView.append(strings[0]);
			return;
		}
	}

	private class GDump extends AsyncTask<Void, String, Void>{

		@Override
		protected Void doInBackground(Void... voids) {

			Cursor resultCursor = mContentResolver.query(mUri, null, "*", null, null);

			while (resultCursor.moveToNext()) {
				String key = resultCursor.getString(resultCursor.getColumnIndex("key"));
				String value = resultCursor.getString(resultCursor.getColumnIndex("value"));
				publishProgress(key + ":" + value+"\n" );
			}

			return null;
		}

		protected void onProgressUpdate(String...strings) {
			mTextView.append(strings[0]);

			return;
		}
	}

	private class Task extends AsyncTask<Void, String, Void> {

		@Override
		protected Void doInBackground(Void... params) {
			if (testInsert()) {
				publishProgress("Insert success\n");
			} else {
				publishProgress("Insert fail\n");
				return null;
			}

			if (testQuery()) {
				publishProgress("Query success\n");
			} else {
				publishProgress("Query fail\n");
			}
			
			return null;
		}
		
		protected void onProgressUpdate(String...strings) {
			mTextView.append(strings[0]);

			return;
		}

		private boolean testInsert() {
			try {
				for (int i = 0; i < TEST_CNT; i++) {
					mContentResolver.insert(mUri, mContentValues[i]);
				}
			} catch (Exception e) {
				Log.e(TAG, e.toString());
				return false;
			}

			return true;
		}

		private boolean testQuery() {
			try {
				for (int i = 0; i < TEST_CNT; i++) {
					String key = (String) mContentValues[i].get(KEY_FIELD);
					String val = (String) mContentValues[i].get(VALUE_FIELD);

					Cursor resultCursor = mContentResolver.query(mUri, null,
							key, null, null);
					if (resultCursor == null) {
						Log.e(TAG, "Result null");
						throw new Exception();
					}

					int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
					int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
					if (keyIndex == -1 || valueIndex == -1) {
						Log.e(TAG, "Wrong columns");
						resultCursor.close();
						throw new Exception();
					}

					resultCursor.moveToFirst();

					if (!(resultCursor.isFirst() && resultCursor.isLast())) {
						Log.e(TAG, "Wrong number of rows");
						resultCursor.close();
						throw new Exception();
					}

					String returnKey = resultCursor.getString(keyIndex);
					String returnValue = resultCursor.getString(valueIndex);
					if (!(returnKey.equals(key) && returnValue.equals(val))) {
						Log.e(TAG, "(key, value) pairs don't match\n");
						resultCursor.close();
						throw new Exception();
					}

					resultCursor.close();
				}
			} catch (Exception e) {
				return false;
			}

			return true;
		}
	}
}
