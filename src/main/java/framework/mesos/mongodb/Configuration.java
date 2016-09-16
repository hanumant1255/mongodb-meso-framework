package framework.mesos.mongodb;



import zookeeper.IpPortsListZKFormatter;
import zookeeper.MesosZKFormatter;
import zookeeper.ZKAddressParser;
import zookeeper.ZKFormatter;

public class Configuration {


	private String version = "1.0.0";
	private double cpus = 1.0;
	private double mem = 256;
	private double disk =512;
	private int port=9000;

	private int configNodes=1;
	private int dbNodes=1;
	private int routerNodes=1;
	private  int mongoNodes = routerNodes + dbNodes + configNodes;
	private String clusterName = "mongoCluster";
	private String frameworkName = "mongoframework";

	private String frameworkRole = "*";   
	private double frameworkFailoverTimeout = 2592000;

	private String  mesosZKURL= "zk://localhost:2181/mesos";
	private long zookeeperMesosTimeout = 20000L;
	private String dockerImage="hanumant/mongodb-base";
	

	public String getDockerImage() {
		return  dockerImage;
	}
	public String getVersion() {
		return version;
	}
	public double getCpus() {
		return cpus;
	}

	public double getMem() {
		return mem;
	}

	public int getport() {
		return port;
	}

	public int getConfigNodes() {
	
		return configNodes;
	}

	public int getRouterNodes() {
		return routerNodes;
	}
	
	
	public Integer getDbNodes() {
		return dbNodes;
	}
	

	public void setDbNodes(int dbNodes) {
		this.dbNodes+=dbNodes;
		mongoNodes+=dbNodes;
		
		
	}
	public void setConfigNodes(int configNodes) {
		this.configNodes+=configNodes;
		mongoNodes+=configNodes;
	
	}
	public void setRouterNodes(int routerNodes) {
		this.routerNodes+=routerNodes;
		mongoNodes+=routerNodes;

	}

	public int getMongoNodes() {
		return mongoNodes;
	}

	public double getDisk() {
		return disk;
	}
	public String getFrameworkName() {
		return frameworkName;
	}

	public double getFailoverTimeout() {
		return frameworkFailoverTimeout;
	}


	public String getframeworkRole(){

		return frameworkRole;

	}

	public String getMesosStateZKURL() {
		ZKFormatter mesosStateZKFormatter = new IpPortsListZKFormatter(new ZKAddressParser());
		return mesosStateZKFormatter.format( mesosZKURL);
	}

	public String getMesosZKURL() {
		ZKFormatter mesosZKFormatter = new MesosZKFormatter(new ZKAddressParser());
		return mesosZKFormatter.format( mesosZKURL);
	}

	public long getZookeeperMesosTimeout() {
		return zookeeperMesosTimeout;
	}
	public String getMongoClusterName()
	{
		return clusterName;
	}
}
