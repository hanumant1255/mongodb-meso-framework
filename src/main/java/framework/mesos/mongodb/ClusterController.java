package framework.mesos.mongodb;




import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;



@RestController
public class ClusterController {



	@Autowired
	Configuration configuration;

	@RequestMapping(value = "/setDbNodes", method = RequestMethod.PUT)
	public int setMongoDbNodes(@RequestBody mongoNodesWrapper dbNodes) {
		configuration.setDbNodes(dbNodes.getValue());
		return configuration.getDbNodes();
	}

	@RequestMapping(value = "/setConfigNodes", method = RequestMethod.PUT)
	public int setMongoConfigNodes(@RequestBody mongoNodesWrapper configNodes) {
		configuration.setConfigNodes(configNodes.getValue());
		return configuration.getConfigNodes();
	}
	@RequestMapping(value = "/setRouterNodes", method = RequestMethod.PUT)
	public int setMongoRouterNodes(@RequestBody mongoNodesWrapper routerNodes) {
		configuration.setRouterNodes(routerNodes.getValue());
		return configuration.getRouterNodes();
	}
	public static class mongoNodesWrapper {
		private int value;
		public int getValue() {
			return value;
		}
		public mongoNodesWrapper() {

		}
		public mongoNodesWrapper(int value) {
			this.value = value;

		}
	}

}
