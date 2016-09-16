package mongodb_mesos_framework;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import static org.mockito.Mockito.*;

import framework.mesos.mongodb.Configuration;

public class ConfigurationTest {
 
	Configuration configuration=new Configuration();
	@Test
	public void shouldReturnValidzkUrl()
	{
		
		 //Configuration configuration =mock(Configuration.class);
		 //when(configuration.getMesosZKURL()).thenReturn("zk://vagrant:2181/mesos");
		 String zkurl="zk://localhost:2181/mesos";
		 assertEquals(zkurl,configuration.getMesosZKURL());
			 
	}

	@Test
	public void shouldReturnValidzkStateUrl()
	
	{
		 //Configuration configuration =mock(Configuration.class);
		 //when(configuration.getMesosStateZKURL()).thenReturn("zk://master:2181/mesos");
		 String zkurl="localhost:2181";
		 assertEquals(zkurl,configuration.getMesosStateZKURL());
			 
	}
	
	 public static void main(String[] args){
		 Result result = JUnitCore.runClasses(ConfigurationTest.class);
	      for (Failure failure : result.getFailures()) {
	          System.out.println(failure.toString());
	       }
	 		
	       System.out.println(result.wasSuccessful());
	    }
	 
	

}
