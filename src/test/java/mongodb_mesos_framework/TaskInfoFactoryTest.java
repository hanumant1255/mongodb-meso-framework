
package mongodb_mesos_framework;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;

import org.apache.mesos.Protos;

import org.apache.mesos.Protos.TaskInfo;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import framework.mesos.mongodb.Clock;
import framework.mesos.mongodb.Configuration;
import framework.mesos.mongodb.ExecutorEnvironmentalVariables;
import framework.mesos.mongodb.Resources;
import framework.mesos.mongodb.TaskInfoFactory;
import state.ClusterState;
import state.FrameworkState;


@RunWith(MockitoJUnitRunner.class)

public class TaskInfoFactoryTest {
	static ArrayList<TaskInfo> taskInfoList=new  ArrayList<TaskInfo>();;
	 
	
    public static final String SLAVEID = "SLAVEID";

    @Mock
    private FrameworkState frameworkState;

    @Mock
    private ClusterState clusterState;

    @Mock
    private Configuration configuration;

    @Mock
    private Clock clock;
 
    ExecutorEnvironmentalVariables  executorEnvironmentVariable=new  ExecutorEnvironmentalVariables(clusterState);
    @Before
    public void before() {
        Protos.FrameworkID frameworkId = Protos.FrameworkID.newBuilder().setValue(UUID.randomUUID().toString()).build();
        when(frameworkState.getFrameworkID()).thenReturn(frameworkId);
       when(configuration.getDbNodes()).thenReturn(1);
        when(configuration.getRouterNodes()).thenReturn(1);
        when(configuration.getConfigNodes()).thenReturn(1);
        when(configuration.getframeworkRole()).thenReturn("some-framework-role");
        when(configuration.getDockerImage()).thenReturn("hanumant/mongodb-basis");
       when(clusterState.getTaskList()).thenReturn(taskInfoList == null ? new ArrayList<>(0) : taskInfoList);
        executorEnvironmentVariable=new  ExecutorEnvironmentalVariables(clusterState);
    }

    @Test
    public void testCreateTaskInfo(){
   
    	
    	 Date now = new DateTime().withDayOfMonth(1).withDayOfYear(1).withYear(1970).withHourOfDay(1).withMinuteOfHour(2).withSecondOfMinute(3).withMillisOfSecond(400).toDate();
         when(clock.now()).thenReturn(now);
         when(clock.nowUTC()).thenReturn(ZonedDateTime.now(ZoneOffset.UTC));
 
        TaskInfoFactory factory = new TaskInfoFactory(clusterState,executorEnvironmentVariable);
        
        Protos.Offer offer = getOffer(frameworkState.getFrameworkID());
        Protos.TaskInfo taskInfo = factory.createTask(configuration, frameworkState,offer, clock);
        assertEquals("mongo-confignode",taskInfo.getName());
       
        
       taskInfoList.add(taskInfo);

        taskInfo = factory.createTask(configuration, frameworkState,offer, clock);

        assertEquals("mongo-routernode",taskInfo.getName());
        
        taskInfoList.add(taskInfo);
     
           taskInfo = factory.createTask(configuration, frameworkState,offer, clock);
           System.out.println(taskInfo);
           assertEquals("mongo-routernode",taskInfo.getName());
        
        
    }


    private Protos.Offer getOffer(Protos.FrameworkID frameworkId) {
        return Protos.Offer.newBuilder()
                                                .setId(Protos.OfferID.newBuilder().setValue(UUID.randomUUID().toString()))
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(SLAVEID))
                                                .setFrameworkId(frameworkId)
                                                .setHostname("localhost")
                                                .addAllResources(asList(
                                                        Resources.singlePortRange(9200, "some-framework-role"),
                                                        Resources.singlePortRange(9300, "some-framework-role"),
                                                        Resources.cpus(1.0, "some-framework-role"),
                                                        Resources.disk(2.0, "some-framework-role"),
                                                        Resources.mem(3.0, "some-framework-role")))
                                            .build();
    }
    




}


