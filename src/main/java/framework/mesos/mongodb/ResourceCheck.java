package framework.mesos.mongodb;

import org.apache.mesos.Protos;
import java.util.List;

public class ResourceCheck {

	String resourceName;
	public  ResourceCheck()
	{
	}
	public ResourceCheck(String resourceName) {
		this.resourceName = resourceName;
	}

	public Boolean isEnough(List<Protos.Resource> resourcesList, double requiredValue) {
		Protos.Resource resource = getResource(resourcesList);
		return resource != null && resource.getScalar() != null && resource.getScalar().getValue() >= requiredValue;
	}

	private Protos.Resource getResource(List<Protos.Resource> resourcesList) {
		for (Protos.Resource resource : resourcesList) {

			if (resource.getName().equals(resourceName)) {
				return resource;
			}
		}
		return null;
	}
}

