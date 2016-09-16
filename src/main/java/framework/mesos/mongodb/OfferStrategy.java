package framework.mesos.mongodb;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Parameter;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskInfoOrBuilder;

import state.ClusterState;

import static java.util.Arrays.asList;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import java.util.Optional;

/**
 * Offer strategy
 */
public class OfferStrategy {
	static Logger LOGGER = Logger.getLogger(OfferStrategy.class);
	public static final int DUMMY_PORT = 80;


	protected  ClusterState clusterState;
	protected Configuration configuration;

	protected List<OfferRule> acceptanceRules = null;

	public OfferStrategy(Configuration configuration, ClusterState clusterState) {
		this.clusterState=clusterState;
		this.configuration = configuration;
		acceptanceRules = asList(
				new OfferRule("Cluster size already fulfilled", offer -> clusterState.getTaskList().size() >= configuration.getMongoNodes()),  
				new OfferRule("Hostname is unresolveable", offer -> !isHostnameResolveable(offer.getHostname())),
				new OfferRule("Offer did not have enough CPU resources", offer -> !isEnoughCPU(configuration, offer.getResourcesList())),
				new OfferRule("Offer did not have enough RAM resources", offer -> !isEnoughRAM(configuration, offer.getResourcesList())),
				new OfferRule("Offer did not have enough disk resources", offer -> !isEnoughDisk(configuration, offer.getResourcesList()))

				);
	}

	protected OfferResult evaluate(Protos.Offer offer) {
		final Optional<OfferRule> decline = acceptanceRules.stream().filter(offerRule -> offerRule.rule.accepts(offer)).limit(1).findFirst();
		if (decline.isPresent()) {
			return OfferResult.decline(decline.get().declineReason);
		}
		LOGGER.info("Accepted offer: " + offer.getHostname());
		return OfferResult.accept();
	}


	protected static class OfferResult {
		final boolean acceptable;
		final Optional<String> reason;

		private OfferResult(boolean acceptable, Optional<String> reason) {
			this.acceptable = acceptable;
			this.reason = reason;
		}

		public static OfferResult accept() {
			return new OfferResult(true, Optional.<String>empty());
		}

		public static OfferResult decline(String reason) {
			return new OfferResult(false, Optional.of(reason));
		}
	}


	protected boolean isEnoughCPU(Configuration configuration, List<Protos.Resource> resourcesList) {
		return new ResourceCheck(Resources.RESOURCE_CPUS).isEnough(resourcesList, configuration.getCpus());
	}

	protected boolean isEnoughRAM(Configuration configuration, List<Protos.Resource> resourcesList) {
		return new ResourceCheck(Resources.RESOURCE_MEM).isEnough(resourcesList, configuration.getMem());
	}

	protected boolean isEnoughDisk(Configuration configuration, List<Protos.Resource> resourcesList) {
		return new ResourceCheck(Resources.RESOURCE_DISK).isEnough(resourcesList, configuration.getDisk());
	}

	protected boolean isHostnameResolveable(String hostname) {
		LOGGER.debug("Attempting to resolve hostname: " + hostname);
		InetSocketAddress address = new InetSocketAddress(hostname, DUMMY_PORT);
		return !address.isUnresolved();
	}


	protected static class OfferRule {
		String declineReason;
		Rule rule;

		public OfferRule(String declineReason, Rule rule) {
			this.declineReason = declineReason;
			this.rule = rule;
		}
	}

	/**
	 * Interface for checking offers
	 */
	@FunctionalInterface
	protected interface Rule {
		boolean accepts(Protos.Offer offer);
	}
}
