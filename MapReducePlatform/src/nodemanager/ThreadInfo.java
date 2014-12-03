package nodemanager;

import java.lang.Thread.UncaughtExceptionHandler;

class ThreadInfo{
	public Thread thread;
	public ThreadCallBack callback;
	public boolean failed;
	
	public UncaughtExceptionHandler exceptionHandler = new UncaughtExceptionHandler(){
		public void uncaughtException(Thread t, Throwable e){
			failed = true;
			System.out.println("thread "+t.getId()+" failed: "+e.getMessage());
		}
	};
	
	ThreadInfo(){
		this.callback = new ThreadCallBack();
		this.failed = false;
	}
	
	class ThreadCallBack{
		public void failJob(){
			failed = true;
		}
	}
}