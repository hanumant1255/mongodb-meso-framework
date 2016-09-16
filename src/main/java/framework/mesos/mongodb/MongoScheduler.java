package framework.mesos.mongodb;
import java.util.*;


import org.apache.log4j.Logger;
import org.apache.mesos.*;
import org.apache.mesos.Protos.*;


import state.ClusterState;
import state.FrameworkState;
import state.SerializableState;
import state.StatePath;
public class MongoScheduler implements Scheduler {

	private  Configuration configuration;
	private  TaskInfoFactory taskInfoFactory;
	private  OfferStrategy offerStrategy;
	private FrameworkState frameworkState;
	private final ClusterState clusterState;
	private SerializableState zookeeperStateDriver;

	List<Protos.Resource> resources=null;
	static Logger LOGGER = Logger.getLogger(MongoScheduler.class);

	public MongoScheduler(Configuration configuration,TaskInfoFactory taskInfoFactory,OfferStrategy offerStrategy,FrameworkState frameworkState,ClusterState clusterState, SerializableState zookeeperStateDriver)
	{
		this.configuration=configuration;
		this.taskInfoFactory=taskInfoFactory;
		this.offerStrategy=offerStrategy;
		this.frameworkState=frameworkState;
		this.clusterState = clusterState;
		this.zookeeperStateDriver = zookeeperStateDriver;

	}

	public void registered(SchedulerDriver driver, FrameworkID frameworkId,
			MasterInfo masterInfo) {

		LOGGER.info("Framework registered as " + frameworkId.getValue());
		resources = Resources.buildFrameworkResources(configuration);

		Protos.Request request = Protos.Request.newBuilder()
				.addAllResources(resources)
				.build();
		List<Protos.Request> requests = Collections.singletonList(request);
		driver.requestResources(requests);
		frameworkState.markRegistered(frameworkId);

	}

	public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
		LOGGER.info("Status update:" +
				" " + status.getSlaveId() +
				" " + status.getExecutorId() +
				" " + status.getHealthy() +
				" " + status.getState() +
				" " + status.getReason() +
				" " +
				" ");
		clusterState.updateTask(status);
	}


	public void resourceOffers(SchedulerDriver driver,List<Protos.Offer> offers) {

		for (Protos.Offer offer : offers) {	

			final OfferStrategy.OfferResult result = offerStrategy.evaluate(offer);

			if (!result.acceptable){
				driver.declineOffer(offer.getId());
				LOGGER.debug("Declined offer: "+ "Reason: " + result.reason);
			} 
			else {
				Clock clock=new Clock();
				Protos.TaskInfo taskInfo =taskInfoFactory.createTask(configuration, frameworkState, offer, clock);
				LOGGER.debug(taskInfo.toString());
				driver.launchTasks(Collections.singleton(offer.getId()), Collections.singleton(taskInfo));
				MdTaskStatus esTask = new MdTaskStatus(zookeeperStateDriver, frameworkState.getFrameworkID(), 
						taskInfo, new StatePath(zookeeperStateDriver));
				// Write staging state to zk
				clusterState.addTask(esTask); // Add tasks to cluster state and write to zk

			}
		}

	}

	public void disconnected(SchedulerDriver driver) { 
		LOGGER.warn("disconnected"+driver.abort());
	}



	public void error(SchedulerDriver driver, java.lang.String message) { 
		LOGGER.error("error\n"+message);
	}



	public void executorLost(SchedulerDriver driver, ExecutorID executorId,
			SlaveID slaveId, int status) { 
		LOGGER.info(("Executor lost:\n " + executorId.getValue() +
				" on slave \n" + slaveId.getValue() +
				" with status \n" + status));
		try {
			Protos.TaskInfo taskInfo = clusterState.getTask(executorId);
			statusUpdate(driver, Protos.TaskStatus.newBuilder().setExecutorId(executorId).setSlaveId(slaveId).setTaskId(taskInfo.getTaskId()).setState(Protos.TaskState.TASK_LOST).build());
			driver.killTask(taskInfo.getTaskId()); // It may not actually be lost, it may just have hanged. So Kill, just in case.
		} catch (IllegalArgumentException e) {
			LOGGER.warn("Unable to find TaskInfo with the given Executor ID");
		}
	}



	public void frameworkMessage(SchedulerDriver driver, ExecutorID executorId,
			SlaveID slaveId, byte[] data) { 
		LOGGER.info("Framework Message - Executor:\n " + executorId.getValue() + ", SlaveID:\n " + slaveId.getValue());
	}



	public void offerRescinded(SchedulerDriver driver, OfferID offerId) { 
		LOGGER.info("Offer \n" + offerId.getValue() + " rescinded");
	}



	public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {
		LOGGER.info("Framework re-registered");
	}



	public void slaveLost(SchedulerDriver driver, SlaveID slaveId) { 
		LOGGER.info("Slave lost: \n" + slaveId.getValue());
	}
	public void run(SchedulerDriver schedulerDriver) {
		LOGGER.info("Starting MongodbFramework on Mesos - [numHwNodes: " + configuration.getMongoNodes() +
				", zk mesos: " + configuration.getMesosZKURL() +
				", ram:" + configuration.getMem() + "]");

		schedulerDriver.run();
	}
	public void shutdown(SchedulerDriver driver) {
		clusterState.getTaskList().stream().forEach(taskInfo -> driver.killTask(taskInfo.getTaskId())); // Kill tasks.
		clusterState.destroy(); // Remove tasks from zk
		frameworkState.destroy(); // Remove framework state from zk.
	}


}
