package framework.mesos.mongodb;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Parameter;
import org.apache.mesos.Protos.TaskInfo;

import state.ClusterState;

public class ExecutorEnvironmentalVariables {
	private static final Logger LOGGER = Logger.getLogger(ExecutorEnvironmentalVariables.class);
	private final static List<Protos.Environment.Variable> envList = new ArrayList<>();

	private  ClusterState clusterState;
	private ArrayList<TaskInfo> taskInfoList;
	public ExecutorEnvironmentalVariables(ClusterState clusterState) {
		this.clusterState = clusterState;

	}

	public void addToList(String port,String data_type,Protos.Offer offer) {

		if(!envList.isEmpty())
			envList.clear();

		envList.add(getEnvProto("PORT_NO",port));
		envList.add(getEnvProto("DATA_TYPE",data_type));

		if(data_type.equals("routernode"))
		{

			String configIpPort=getIpPortString("confignode");
			envList.add(getEnvProto("CONFIGIP_PORT",configIpPort));
			LOGGER.info("\nconfigipport\n"+configIpPort);

		}
		else  if(data_type.equals("dbnode")){
			
			String routerIpPort=getIpPortString("routernode");
			LOGGER.info("\nrouteripport\n"+routerIpPort);
			envList.add(getEnvProto("ROUTERIP_PORT",routerIpPort));

			final  String dbhostAddress= resolveHostAddress(offer,27010);
			String dbIpPort =dbhostAddress+":"+port;
			envList.add(getEnvProto("DBIP_PORT",dbIpPort));
			LOGGER.info("\ndbipport\n"+dbIpPort);

		}


	}


	public List<Protos.Environment.Variable> getList() {
		return envList;
	}

	private Protos.Environment.Variable getEnvProto(String key, String value) {

		return Protos.Environment.Variable.newBuilder()
				.setName(key)
				.setValue(value).build();
	}
	private String resolveHostAddress(Protos.Offer offer, int string) {
		String hostname = offer.getHostname();
		InetSocketAddress address = new InetSocketAddress(hostname, string);
		return address.getAddress().getHostAddress();
	}
	private String getIpPortString(String typeOfNode)
	{
		String[] portNo=null;
		String[] dataType=null;
		String hostAddress=null;

		List<String>port=new ArrayList<String>();
		List<String>ip=new ArrayList<String>();
		taskInfoList =clusterState.getTaskList();


		for (TaskInfo resource :taskInfoList) {
			List<Parameter> parameterList=resource.getContainer().getDockerOrBuilder().getParametersList();
			Properties data = new Properties();
			portNo=parameterList.get(0).getValue().split("=");
			dataType=parameterList.get(1).getValue().split("=");
			try {
				data.load(resource.getData().newInput());

			} catch (IOException e) {
				e.printStackTrace();
			}

			hostAddress=data.getProperty("ipAddress");

			if(dataType[1].equals(typeOfNode)){
				port.add(portNo[1]);
				ip.add(hostAddress);
			}

		}
		if(typeOfNode.equals("routernode"))
			return ip.get(0)+":"+port.get(0);	

		return getIpPort(port,ip);

	}
	public String getIpPort(List<String>portList,List<String>ipList){
		String ipPortString=null;
		for(int i=0;i<portList.size();i++)
		{
			if(i!=0)
				ipPortString=ipPortString+","; 
			if(i==0)
				ipPortString=ipList.get(i)+":"+portList.get(i);
			else
				ipPortString=ipPortString+ipList.get(i)+":"+portList.get(i);  
		}


		return ipPortString;

	}
}