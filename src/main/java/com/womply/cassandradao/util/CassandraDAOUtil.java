package com.womply.cassandradao.util;

import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

/* 
 * Loads the properties from cassandradao.properties
 * Define your contact points, username, password, ...
 * 
 * CassandraDAOUtil is unaware of any encryption/decryption policies.
 * We may need to define such policies.
 */
public class CassandraDAOUtil {

	private static Logger LOGGER = LoggerFactory
			.getLogger(CassandraDAOUtil.class);

	private static Properties cassandraDaoProperties = new Properties();

	public static Session getSession() {

		Session session = null;

		try {
			cassandraDaoProperties.load(CassandraDAOUtil.class
					.getResourceAsStream(Constants.CASSANDRA_DAO_PROPERTIES));

			int port = Integer.parseInt(cassandraDaoProperties
					.getProperty(Constants.PORT));

			String[] contactPoints = cassandraDaoProperties.getProperty(
					Constants.CONTACT_POINTS).split(Constants.COMMA);

			Builder builder = Cluster
					.builder()
					.addContactPoints(contactPoints)
					.withPort(port)
					.withReconnectionPolicy(
							new ExponentialReconnectionPolicy(
									Constants.BASE_DELAY_MS,
									Constants.MAX_DELAY_MS))
					.withSocketOptions(new SocketOptions().setKeepAlive(true))
					.withCredentials(
							cassandraDaoProperties
									.getProperty(Constants.USERNAME),
							cassandraDaoProperties
									.getProperty(Constants.PASSWORD));

			if (cassandraDaoProperties.containsKey("LoadBalancingPolicy")) {
				builder.withLoadBalancingPolicy(getLoadBalancingPolicy(cassandraDaoProperties
						.getProperty("LoadBalancingPolicy")));
			} else {
				LOGGER.warn("LoadBalancingPolicy not configured...");
			}

			if (cassandraDaoProperties.getProperty(Constants.SSL).equals("ON")) {
				LOGGER.info("SSL turned on...");
				builder = builder.withSSL();
			} else {
				LOGGER.info("SSL OFF...");
			}

			Cluster cluster = builder.build().init();

			session = cluster.connect();

			LOGGER.info("Acquired Cassandra Session for ips - {}",
					cassandraDaoProperties
							.getProperty(Constants.CONTACT_POINTS));
		} catch (IOException e) {
			LOGGER.error("Properties file may not be configured", e);
		}
		return session;
	}

	/* UNUSED : have not used the load balancing policies yet */
	private static LoadBalancingPolicy getLoadBalancingPolicy(String policy) {

		policy = policy.trim();
		LOGGER.warn("LoadBalancingPolicy configured... {}", policy);
		if (policy.equals("TAP_DCARRP") || policy.length() == 0) {
			LOGGER.warn(
					"LoadBalancingPolicy set as TokenAwarePolicy (DCAwareRoundRobinPolicy({}))",
					cassandraDaoProperties.getProperty(Constants.DC));
			return new TokenAwarePolicy(new DCAwareRoundRobinPolicy(
					cassandraDaoProperties.getProperty(Constants.DC)));
		} else if (policy.equals("TAP_RRP")) {
			LOGGER.warn("LoadBalancingPolicy set as TokenAwarePolicy (RoundRobinPolicy())");
			return new TokenAwarePolicy(new RoundRobinPolicy());
		} else if (policy.equals("RRP")) {
			LOGGER.warn("LoadBalancingPolicy set as RoundRobinPolicy()");
			return new RoundRobinPolicy();
		} else if (policy.contains("TAP_DCARRP_")) {
			String[] arr = policy.split("TAP_DCARRP_");

			LOGGER.warn(
					"LoadBalancingPolicy set as TokenAwarePolicy (DCAwareRoundRobinPolicy({},{}))",
					cassandraDaoProperties.getProperty(Constants.DC),
					Integer.parseInt(arr[1].trim()));

			return new TokenAwarePolicy(new DCAwareRoundRobinPolicy(
					cassandraDaoProperties.getProperty(Constants.DC),
					Integer.parseInt(arr[1].trim())));
		} else if (policy.contains("DCARRP_")) {
			String[] arr = policy.split("DCARRP_");

			LOGGER.warn(
					"LoadBalancingPolicy set as DCAwareRoundRobinPolicy({},{})",
					cassandraDaoProperties.getProperty(Constants.DC),
					Integer.parseInt(arr[1].trim()));

			return new DCAwareRoundRobinPolicy(
					cassandraDaoProperties.getProperty(Constants.DC),
					Integer.parseInt(arr[1].trim()));
		}
		return Policies.defaultLoadBalancingPolicy();
	}
}
