package fr.visioterra.lib.tools;

import java.util.EventListener;

import javax.swing.event.EventListenerList;


/*
 * Changes :
 * - 2009/06/04		|	Grégory Mazabraud		|	class creation
 * - 2011/04/16		|	Grégory Mazabraud		|	add "cancel" process
 * - 2022/04/28		|	Grégory Mazabraud		|	add subTaskMonitor(...) and ensureNotNull(...)
 */
public class TaskMonitor {
	
	
	public static class CancelTaskException extends RuntimeException {

		private static final long serialVersionUID = 112616485817538253L;

		public CancelTaskException() {
			super();
		}
		
	}
	

	//inner class
	public enum Status {
		PENDING,
		STARTED,
		DONE,
		CANCELLED,
		ERROR
	}

	public class StatusEvent {
		private final Status oldStatus;
		private final Status newStatus;
		
		public StatusEvent(Status oldStatus, Status newStatus) {
			this.oldStatus = oldStatus;
			this.newStatus = newStatus;
		}

		public Status getOldStatus() {
			return oldStatus;
		}

		public Status getNewStatus() {
			return newStatus;
		}
		
		public TaskMonitor getTask() {
			return TaskMonitor.this;
		}
		
	}
	
	public class ProgressEvent {
		private final int oldProgress;
		private final int newProgress;
		
		public ProgressEvent(int oldProgress, int newProgress) {
			this.oldProgress = oldProgress;
			this.newProgress = newProgress;
		}

		public int getOldProgress() {
			return oldProgress;
		}

		public int getNewProgress() {
			return newProgress;
		}
		
		public TaskMonitor getTask() {
			return TaskMonitor.this;
		}
	}
	
	public interface TaskMonitorListener extends EventListener {
		public void statusChange(StatusEvent e);
		public void progressChange(ProgressEvent e);
	}
	
	
	//fields
	private final EventListenerList listeners;
	private final Object lock = new Object();
	private final boolean throwEvents;
	private int min;
	private int max;
	private int progress; // = 0;
	private Status status;
	private String label;


	private void fireStatusChanged(Status oldStatus, Status newStatus) {
		if(this.throwEvents) {
			StatusEvent event = null;
			for(TaskMonitorListener listener : getTaskListeners()) {
				if(event == null)
					event = new StatusEvent(oldStatus, newStatus);
				listener.statusChange(event);
			}
		}
	}
	
	private void fireProgressChanged(int oldProgress, int newProgress) {
		if(this.throwEvents) {
			ProgressEvent event = null;
			for(TaskMonitorListener listener : getTaskListeners()) {
				if(event == null)
					event = new ProgressEvent(oldProgress, newProgress);
				listener.progressChange(event);
			}
		}
	}
	
	public TaskMonitor(boolean throwEvents) {
		this(0,100,throwEvents);
	}
	
	public TaskMonitor(int min, int max, boolean throwEvents) {
		this.throwEvents = throwEvents;
		this.min = min;
		this.max = max;
		this.progress = this.min;
		this.status = Status.PENDING;
		this.listeners = throwEvents ? new EventListenerList() : null;
	}
	
	public Status getStatus() {
		synchronized(this.lock) {
			return this.status;
		}
	}
	
	public boolean isCancelled() {
		return status == Status.CANCELLED;
	}
	
	public void checkCancelled() throws CancelTaskException {
		if (status == Status.CANCELLED) {
			throw new CancelTaskException();
		}
	}
	
	public void setStatus(Status status) {
		synchronized(this.lock) {
			
			if(this.status == status) {
				return;
			}

			Status old = this.status;
			this.status = status;
			fireStatusChanged(old, status);
		}
	}
	
	public void initProgress(int min, int max) {
		synchronized(this.lock) {
			this.min = min;
			this.max = max;
			this.progress = min;
		}
	}
	
	public int getMin() {
		return min;
	}

	public int getMax() {
		return max;
	}
	
	public int getProgress() {
		synchronized(this.lock) {
			return this.progress;
		}
	}
	
	public void setProgress(int progress) {
		synchronized(this.lock) {
			
			if(this.progress == progress) {
				return;
			}

			if(progress < this.min || this.max < progress) {
				return;
			}

			int tmp = this.progress;
			this.progress = progress;
			fireProgressChanged(tmp,progress);
		}
	}
	
	public int getPercentage() {
		return (getProgress() - min) * 100 / (max - min);
	}

	public String getLabel() {
		return this.label;
	}
	
	public void setLabel(String label) {
		this.label = label;
	}
	
	public void addTaskListener(TaskMonitorListener listener) {
		if(throwEvents) {
			synchronized(this.lock) {
				listeners.add(TaskMonitorListener.class,listener);
			}
		}
	}
	
	public void removeTaskListener(TaskMonitorListener listener) {
		if(this.throwEvents) {
			synchronized(this.lock) {
				listeners.remove(TaskMonitorListener.class,listener);
			}
		}
	}
	
	public TaskMonitorListener[] getTaskListeners() {
		if(throwEvents) {
			synchronized(this.lock) {
				return listeners.getListeners(TaskMonitorListener.class);
			}
		}
		else {
			return null;
		}
	}
	
	public int getTaskListenerCount() {
		if(throwEvents) {
			synchronized(this.lock) {
				return listeners.getListenerCount(TaskMonitorListener.class);
			}
		}
		else {
			return 0;
		}
	}

	public TaskMonitor subTaskMonitor(int min, int max) {
		
		TaskMonitor sub = new TaskMonitor(this.throwEvents);
		
		if(this.throwEvents) {
			sub.addTaskListener(new TaskMonitorListener() {
				@Override public void statusChange(StatusEvent e) { }
				@Override public void progressChange(ProgressEvent e) {
					TaskMonitor.this.setLabel(sub.getLabel());
//					TaskMonitor.this.setProgress(offset + sub.getPercentage());
					TaskMonitor.this.setProgress(min + (max-min) * sub.getPercentage() / 100);
				}
			});
		}
		
		return sub;
	}
	
	public static TaskMonitor ensureNotNull(TaskMonitor monitor) {
		if(monitor == null) {
			monitor = new TaskMonitor(false);
		}
		return monitor;
	}
	
}
