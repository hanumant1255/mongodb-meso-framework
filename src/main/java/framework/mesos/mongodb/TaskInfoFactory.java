package framework.mesos.mongodb;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;

import com.google.protobuf.ByteString;


import state.ClusterState;
import state.FrameworkState;

import org.apache.mesos.Protos.ContainerInfo.Builder;
import org.apache.mesos.Protos.Resource;


public class TaskInfoFactory {
	private static final Logger LOGGER = Logger.getLogger(TaskInfoFactory.class);
	public static final String TASK_DATE_FORMAT = "yyyyMMdd'T'HHmmss.SSS'Z'";

	private ArrayList<TaskInfo> taskInfoList;
	  
	private ClusterState clusterState;
	private ExecutorEnvironmentalVariables executorEnvironmentVariable;

	public TaskInfoFactory(ClusterState clusterState, ExecutorEnvironmentalVariables executorEnvironmentVariable) {
		this.clusterState = clusterState;
		this.executorEnvironmentVariable = executorEnvironmentVariable;
	}

	

	public  TaskInfo createTask(Configuration configuration, FrameworkState frameworkState, Protos.Offer offer,Clock clock) {

		Integer port=selectOnePortFromRange(offer.getResourcesList());

		final List<Protos.Resource> resources = getResources(configuration, port);
		LOGGER.info("Creating  task with resources: " + resources.toString());
		final  String hostAddress = resolveHostAddress(offer,27010);
		final Protos.TaskID taskId = Protos.TaskID.newBuilder().setValue(taskId(offer, clock)).build();

		return TaskInfo.newBuilder() 
				.setName("mongo-"+getTaskType(configuration))
				.setTaskId(taskId)
				.setData(toData(offer.getHostname(), hostAddress, clock.nowUTC()))
				.addAllResources(resources)
				.setContainer(getContainerinfo(offer, configuration,executorEnvironmentVariable))
				.setCommand(Protos.CommandInfo.newBuilder().setShell(false))
				.setSlaveId(offer.getSlaveId())
				.build();
	}
	private List<Protos.Resource> getResources(Configuration configuration,Integer port) {
		List<Protos.Resource> acceptedResources = Resources.buildFrameworkResources(configuration);
		acceptedResources.add(Resources.singlePortRange(port, configuration.getframeworkRole()));
		return acceptedResources;
	}

	public ByteString toData(String hostname, String ipAddress, ZonedDateTime zonedDateTime) {
		Properties data = new Properties();
		data.put("hostname", hostname);
		data.put("ipAddress", ipAddress);
		data.put("startedAt", zonedDateTime.toString());

		StringWriter writer = new StringWriter();
		try {
			data.store(new PrintWriter(writer), "Task metadata");
		} catch (IOException e) {
			throw new RuntimeException("Failed to write task metadata", e);
		}
		return ByteString.copyFromUtf8(writer.getBuffer().toString());
	}
	private String taskId(Protos.Offer offer, Clock clock) {
		String date = new SimpleDateFormat(TASK_DATE_FORMAT).format(clock.now());
		return String.format("mongodb_%s_%s", offer.getHostname(), date);
	}
	private String resolveHostAddress(Protos.Offer offer, int string) {
		String hostname = offer.getHostname();

		InetSocketAddress address = new InetSocketAddress(hostname, string);
		return address.getAddress().getHostAddress();
	}


	private String getTaskType (Configuration configuration) {
		String tasktype = "dbnode";
		int instanceCount=getMongoInstanceCount("confignode");
		if(instanceCount<configuration.getConfigNodes()) { 
			tasktype="confignode";
			LOGGER.info("launching "+tasktype);
			return tasktype;
		}
		instanceCount=getMongoInstanceCount("routernode"); 
		if(instanceCount<configuration.getRouterNodes()) {
			tasktype="routernode";
			LOGGER.info("launching "+tasktype);
			return tasktype;
		} 
		instanceCount=getMongoInstanceCount("dbnode");
		if(instanceCount<configuration.getDbNodes()) {
			tasktype="dbnode";
			LOGGER.info("launching "+tasktype);
			return tasktype;
			
		}
		LOGGER.info("launching "+tasktype);
		return tasktype; 
	}

	protected Builder getContainerinfo(Protos.Offer offer, Configuration configuration,
			ExecutorEnvironmentalVariables executorEnvironmentVariable) {

		String port = Integer.toString(selectOnePortFromRange(offer.getResourcesList()));

		executorEnvironmentVariable.addToList(port, getTaskType(configuration),offer);

		final Protos.Environment environment = Protos.Environment.newBuilder()
				.addAllVariables(executorEnvironmentVariable.getList()).build();
		Protos.ContainerInfo.DockerInfo.Builder dockerInfoBuilder = Protos.ContainerInfo.DockerInfo.newBuilder();
		dockerInfoBuilder.setImage(configuration.getDockerImage());
		dockerInfoBuilder.setNetwork(Protos.ContainerInfo.DockerInfo.Network.HOST);
		dockerInfoBuilder.setForcePullImage(true);
		dockerInfoBuilder.setPrivileged(true);

		for (Protos.Environment.Variable variable : environment.getVariablesList()) {
			dockerInfoBuilder.addParameters(Protos.Parameter.newBuilder().setKey("env")
					.setValue(variable.getName() + "=" + variable.getValue()));
		}
		Protos.ContainerInfo.Builder containerInfoBuilder = Protos.ContainerInfo.newBuilder();
		containerInfoBuilder.setType(Protos.ContainerInfo.Type.DOCKER);
		containerInfoBuilder.setDocker(dockerInfoBuilder.build());
		return containerInfoBuilder;
	}


	public static Integer selectOnePortFromRange(List<Resource> list) {

		return list.stream().filter(resource -> resource.getType().equals(org.apache.mesos.Protos.Value.Type.RANGES))
				.filter(resource -> resource.getRanges().getRangeList().size() > 0)
				.findAny().flatMap(resource -> resource.getRanges().getRangeList().stream().findAny()).map(range -> (int) range.getBegin()).get();

	}

	public int getMongoInstanceCount(String LDATA_TYPE) {

		int mongoInstanceCount = 0;
		taskInfoList = clusterState.getTaskList();
		if (taskInfoList.isEmpty())
			return 0;
		String[] DATA_TYPE = null;
		for (TaskInfo resource : taskInfoList) {
			List<org.apache.mesos.Protos.Parameter> parameterList = resource.getContainer().getDockerOrBuilder()
					.getParametersList();
			DATA_TYPE = parameterList.get(1).getValue().split("=");
			if (DATA_TYPE[1].equals(LDATA_TYPE))
				mongoInstanceCount++;
		}

		return mongoInstanceCount;
	}
}
