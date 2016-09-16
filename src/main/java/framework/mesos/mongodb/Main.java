package framework.mesos.mongodb;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.state.ZooKeeperState;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;



import state.ClusterState;
import state.FrameworkState;
import state.SerializableZookeeperState;

@SpringBootApplication
public class Main {

	private static Configuration configuration;
	static Logger LOGGER = Logger.getLogger(Main.class);
	public static void main(String ... args) 
	{  
		//SpringApplication.run(Main.class, args);
		configuration = new Configuration();	
		final SerializableZookeeperState zookeeperStateDriver = new SerializableZookeeperState(new ZooKeeperState(
				configuration.getMesosStateZKURL(),
				configuration.getZookeeperMesosTimeout(),
				TimeUnit.MILLISECONDS,
				"/" + configuration.getFrameworkName() + "/" + configuration.getMongoClusterName()));


		final FrameworkState frameworkState = new FrameworkState(zookeeperStateDriver);
		final ClusterState clusterState = new ClusterState(zookeeperStateDriver, frameworkState);


		final OfferStrategy offerStrategy=new OfferStrategy (configuration,clusterState);

		final ExecutorEnvironmentalVariables executorEnvironmentVariable= new ExecutorEnvironmentalVariables(clusterState); 
		final TaskInfoFactory taskInfoFactory = new TaskInfoFactory(clusterState, executorEnvironmentVariable);

		final MongoScheduler scheduler = new MongoScheduler(configuration,taskInfoFactory, offerStrategy,frameworkState,clusterState,zookeeperStateDriver);  

		FrameworkInfoFactory frameworkInfoFactory = new FrameworkInfoFactory(configuration, frameworkState);

		final Protos.FrameworkInfo.Builder frameworkBuilder = frameworkInfoFactory.getBuilder();

		final MesosSchedulerDriver schedulerDriver;

		schedulerDriver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), configuration.getMesosZKURL());

	
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				LOGGER.info("Performing graceful shutdown");

				scheduler.shutdown(schedulerDriver);
			}
		});
		 HashMap<String, Object> properties = new HashMap<>();
	        properties.put("server.port", String.valueOf(configuration.getport()));
	        new SpringApplicationBuilder(Main.class)
	                .properties(properties)
	                .initializers(applicationContext -> applicationContext.getBeanFactory().registerSingleton("configuration", configuration))
	                .run(args);
		scheduler.run(schedulerDriver);

	}
}
