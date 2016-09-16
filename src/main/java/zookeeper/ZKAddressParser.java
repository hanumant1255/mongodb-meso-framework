package zookeeper;

import java.awt.List;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
/**
 * Validates ZK url and parses ZK addresses.
 *
 * IMPORTANT: Different components in the framework require different ZK address strings.
 *
 * 1) The ZK State requires a ZK servers url
 *
 * host1:port1,host2:port2
 *
 * 2) The MesosSchedulerDriver requires a full ZK url
 *
 * zk://host1:port1,host2:port2/mesos
 */
public class ZKAddressParser {
	public static final String ZK_PREFIX_REGEX = "^" + ZKAddress.ZK_PREFIX + ".*";

	public ArrayList<ZKAddress> validateZkUrl(final String zkUrl) {
		final ArrayList<ZKAddress> zkList = new ArrayList<>();

		// Ensure that string is prefixed with "zk://"
		Matcher matcher = Pattern.compile(ZK_PREFIX_REGEX).matcher(zkUrl);
		if (!matcher.matches()) {
			throw new ZKAddressException(zkUrl);
		}

		if (StringUtils.countMatches(zkUrl, '/') < 3) {
			throw new ZKAddressException(zkUrl);
		}

		// Strip zk prefix and spaces
		String zkStripped = zkUrl.replace(ZKAddress.ZK_PREFIX, "").replace(" ", "");

		// Split address by commas
		String[] split = zkStripped.split(",");

		// Validate and add each split
		for (String s : split) {
			zkList.add(new ZKAddress(s));
		}

		// Return list of zk addresses
		return zkList;
	}
}
