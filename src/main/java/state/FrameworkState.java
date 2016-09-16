package state;
import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

import java.awt.List;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;


public class FrameworkState {
	private static final Logger LOGGER = Logger.getLogger(FrameworkState.class);

	private static final String FRAMEWORKID_KEY = "frameworkId";

	public static final Protos.FrameworkID EMPTY_ID = Protos.FrameworkID.newBuilder().setValue("").build();

	private AtomicBoolean registered = new AtomicBoolean(false);

	private final SerializableState zookeeperStateDriver;

	private final StatePath statePath;



	public FrameworkState(SerializableState zookeeperStateDriver) {

		this.zookeeperStateDriver = zookeeperStateDriver;
		statePath = new StatePath(zookeeperStateDriver);

	}

	public Protos.FrameworkID getFrameworkID() {
		Protos.FrameworkID id = null;
		try {
			id = zookeeperStateDriver.get(FRAMEWORKID_KEY);
		} catch (IOException e) {
			LOGGER.warn("Unable to get FrameworkID from zookeeper");
		}
		return id == null ? EMPTY_ID : id;
	}

	public void markRegistered(Protos.FrameworkID frameworkId) {

		if (!registered.compareAndSet(false, true)) {

			throw new IllegalStateException("Framework can not be marked as registered twice");
		}
		try {

			statePath.mkdir(FRAMEWORKID_KEY);
			zookeeperStateDriver.set(FRAMEWORKID_KEY,frameworkId);
			LOGGER.info("FrameworkID stored in zookeeper: " + FRAMEWORKID_KEY + " = " +frameworkId);
		} catch (IOException e) {
			LOGGER.error("Unable to store framework ID in zookeeper");
		}

	
	}

	public void destroy() {
		try {
			statePath.rm(FRAMEWORKID_KEY);
		} catch (IOException e) {
			LOGGER.error("Unable to delete " + FRAMEWORKID_KEY + " from zookeeper");
		}
	}



}
