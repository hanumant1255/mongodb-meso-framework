package mongodb_mesos_framework;
import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import org.apache.mesos.state.ZooKeeperState;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import static org.mockito.Mockito.*;
import framework.mesos.mongodb.Configuration;
import state.SerializableZookeeperState;
public class MainTest {
	Configuration configuration =mock(Configuration.class);
	@Test
	public void checkzkStatedriver(){
		
		
	
	    when(configuration.getMesosStateZKURL()).thenReturn("localhost:2181");
        when(configuration.getZookeeperMesosTimeout()).thenReturn(20000L);
        when(configuration.getFrameworkName()).thenReturn("mongoframework");
        when(configuration.getMongoClusterName()).thenReturn("mongoCluster");
        
		final SerializableZookeeperState zookeeperStateDriver = new SerializableZookeeperState(new ZooKeeperState(
				configuration.getMesosStateZKURL(),
				configuration.getZookeeperMesosTimeout(),
				TimeUnit.MILLISECONDS,
				"/" + configuration.getFrameworkName() + "/" + configuration.getMongoClusterName()));
		
		 assertNotNull(zookeeperStateDriver);
	 
	}
	 public static void main(String[] args){
		 Result result = JUnitCore.runClasses(MainTest.class);
	      for (Failure failure : result.getFailures()) {
	          System.out.println(failure.toString());
	       }
	 		
	       System.out.println(result.wasSuccessful());
	    }
	 

}
