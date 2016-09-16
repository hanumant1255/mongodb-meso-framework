package state;


import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;

//import com.sun.istack.internal.NotNull;

import framework.mesos.mongodb.MdTaskStatus;

public class ClusterState {
	private static final Logger LOGGER = Logger.getLogger(ClusterState.class);
	public static final String STATE_LIST = "stateList";

	private SerializableState zooKeeperStateDriver;
	private FrameworkState frameworkState;

	public ClusterState( SerializableState zooKeeperStateDriver, FrameworkState frameworkState)
	{
		if (zooKeeperStateDriver == null || frameworkState == null) {
			throw new NullPointerException();
		}
		this.zooKeeperStateDriver = zooKeeperStateDriver;
		this.frameworkState = frameworkState;


	}


	public void addTask(MdTaskStatus esTask) {
		addTask(esTask.getTaskInfo());
	}

	public void addTask(TaskInfo taskInfo) {
		LOGGER.info("Adding TaskInfo to cluster for task: " + taskInfo.getTaskId().getValue());

		if (exists(taskInfo.getTaskId())) {
			removeTask(taskInfo);
		}
		ArrayList<TaskInfo> taskList = getTaskList();
		taskList.add(taskInfo); 
		setTaskInfoList(taskList);
	}
	public Boolean exists(TaskID taskId) {
		try {
			getTask(taskId);
		} catch (IllegalArgumentException e) {
			return false;
		}
		return true;
	}

	public TaskInfo getTask(TaskID taskID) throws IllegalArgumentException {
		ArrayList<TaskInfo> taskInfoList = getTaskList();
		TaskInfo taskInfo = null;
		for (TaskInfo info : taskInfoList) {
			if (info.getTaskId().getValue().equals(taskID.getValue())) {
				taskInfo = info;
				break;
			}
		}
		if (taskInfo == null) {
			throw new IllegalArgumentException("Could not find executor with that task ID: " + taskID.getValue());
		}
		return taskInfo;
	}
	public TaskInfo getTask(Protos.ExecutorID executorID) throws IllegalArgumentException {
		if (executorID.getValue().isEmpty()) {
			throw new IllegalArgumentException("ExecutorID.value() is blank. Cannot be blank.");
		}
		ArrayList<TaskInfo> taskInfoList = getTaskList();
		TaskInfo taskInfo = null;
		for (TaskInfo info : taskInfoList) {
			if (info.getExecutor().getExecutorId().getValue().equals(executorID.getValue())) {
				taskInfo = info;
				break;
			}
		}
		if (taskInfo == null) {
			throw new IllegalArgumentException("Could not find executor with that executor ID: " + executorID.getValue());
		}
		return taskInfo;
	}

	public ArrayList<TaskInfo> getTaskList() {
		ArrayList<TaskInfo> taskInfoList = null;
		try {
			taskInfoList = zooKeeperStateDriver.get(getKey());
		} catch (IOException e) {
			LOGGER.info("Unable to get key for cluster state due to invalid frameworkID.");
		}
		return taskInfoList == null ? new ArrayList<>(0) : taskInfoList;
	}

	private String getKey() {
		return frameworkState.getFrameworkID().getValue() + "/" + STATE_LIST;
	}

	public void removeTask(TaskInfo taskInfo) throws InvalidParameterException {
		ArrayList<TaskInfo> taskList = getTaskList();
		LOGGER.info("Removing TaskInfo from cluster for task: " + taskInfo.getTaskId().getValue());
		if (!taskList.remove(taskInfo)) {
			throw new InvalidParameterException("TaskInfo does not exist in list: " + taskInfo.getTaskId().getValue());
		}
		getStatus(taskInfo).destroy(); // Destroy task status in ZK.
		setTaskInfoList(taskList); // Remove from cluster state list
	}
	public MdTaskStatus getStatus(TaskID taskID) throws IllegalArgumentException {
		return getStatus(getTask(taskID));
	}

	private MdTaskStatus getStatus(TaskInfo taskInfo) {
		return new MdTaskStatus(zooKeeperStateDriver, frameworkState.getFrameworkID(), taskInfo, new StatePath(zooKeeperStateDriver));
	}

	private void setTaskInfoList(ArrayList<TaskInfo> taskList) {
		LOGGER.info("Writing executor state list: " + logTaskList(taskList));
		try {
			new StatePath(zooKeeperStateDriver).mkdir(getKey());
			zooKeeperStateDriver.set(getKey(), taskList);
		} catch (IOException ex) {
			LOGGER.error("Could not write list of executor states to zookeeper: ");
		}
	}

	private String logTaskList(ArrayList<TaskInfo> taskList) {
		ArrayList<String> res = new ArrayList<>();
		for (TaskInfo t : taskList) {
			res.add(t.getTaskId().getValue());
		}
		return Arrays.toString(res.toArray());
	}
	/**
	 * Deletes all tasks and state.
	 */
	public void destroy()
	{
		try {
			getTaskList().stream().forEach(taskInfo -> getStatus(taskInfo).destroy());
			zooKeeperStateDriver.delete(getKey());
			zooKeeperStateDriver.delete(frameworkState.getFrameworkID().getValue() + "/" + MdTaskStatus.STATE_KEY);
			zooKeeperStateDriver.delete(frameworkState.getFrameworkID().getValue());
		} catch (IOException e) {
			LOGGER.error("Unable to delete state from ZooKeeper");
		}
	}

	public void update(Protos.TaskStatus status)  throws IllegalArgumentException {
		if (!exists(status.getTaskId())) {
			throw new IllegalArgumentException("Task does not exist in zk.");
		}
		getStatus(status.getTaskId()).setStatus(status);
	}
	public boolean taskInError(Protos.TaskStatus status) {
		return getStatus(status.getTaskId()).taskInError();
	}

	public void updateTask(Protos.TaskStatus status) 
	{
		if (!exists(status.getTaskId())) 
		{
			LOGGER.warn("Could not find task in cluster state.");
			return;
		}

		try {
			Protos.TaskInfo taskInfo = getTask(status.getTaskId());
			LOGGER.info("Updating task status for executor: " + status.getExecutorId().getValue() + " [" + status.getTaskId().getValue() + ", " + status.getTimestamp() + ", " + status.getState() + "]");
			update(status); // Update state of Executor

			if (taskInError(status)) {
				LOGGER.error("Task in error state. Removing state for executor: " + status.getExecutorId().getValue() + ", due to: " + status.getState());
				removeTask(taskInfo); // Remove task from cluster state.
			}
		} catch (IllegalStateException | IllegalArgumentException e) {
			LOGGER.error("Unable to write executor state to zookeeper");
		}
	}

}
