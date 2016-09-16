package framework.mesos.mongodb;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;

/**
 * @param Used for mocking time related code.
 */
public class Clock {

	public Date now() {
		return new Date();
	}

	public ZonedDateTime zonedNow() {
		return ZonedDateTime.now();
	}

	public ZonedDateTime nowUTC() {
		return ZonedDateTime.now(ZoneOffset.UTC);
	}
}
