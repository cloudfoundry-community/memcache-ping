package cloudfoundry.memcache.ping;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.ConnectionFactoryBuilder.Protocol;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.auth.PlainCallbackHandler;
import org.apache.commons.lang.StringUtils;
import org.hibernate.validator.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@SpringBootApplication
@EnableScheduling
public class MemcachePingApplication implements AutoCloseable {
	private static final Logger LOGGER = LoggerFactory.getLogger(MemcachePingApplication.class);

	private final Map<String, MemcachedClient> clients = new HashMap<>();

	public MemcachePingApplication(MemcachePingConfig config) throws Exception {

		ConnectionFactoryBuilder binaryConnectionFactory = new ConnectionFactoryBuilder();
		binaryConnectionFactory.setProtocol(Protocol.BINARY);
		binaryConnectionFactory.setAuthDescriptor(new AuthDescriptor(new String[] { "PLAIN" },
				new PlainCallbackHandler(config.getMemcache().getUsername(), config.getMemcache().getPassword())));
		binaryConnectionFactory.setShouldOptimize(false);

		clients.put("all", new net.spy.memcached.MemcachedClient(binaryConnectionFactory.build(),
				AddrUtil.getAddresses(StringUtils.join(config.getMemcache().getServers(), ' '))));

		for (String server : config.getMemcache().getServers()) {
			clients.put(server, new MemcachedClient(binaryConnectionFactory.build(),
					AddrUtil.getAddresses(server)));
		}
	}

	public static void main(String[] args) {
		SpringApplication.run(MemcachePingApplication.class, args);
		LOGGER.info("Memcache Ping Started.");
	}

	@Scheduled(fixedDelayString = "${pingInterval:5000}")
	public void scheduledTask() {
		for (Map.Entry<String, MemcachedClient> clientEntry : clients.entrySet()) {
			String key = UUID.randomUUID().toString();
			try {
				clientEntry.getValue().set(key, 5, key).get();
				try {
					long time = System.nanoTime();
					if (key.equals(clientEntry.getValue().get(key))) {
						long total = System.nanoTime() - time;
						LOGGER.info("SUCCESS server={} time_ns={}",clientEntry.getKey(), total);
					} else {
						throw new IllegalStateException(
								"Value returned didn't match expected value.  WHAT IS GOING ON!");
					}
				} finally {
					clientEntry.getValue().delete(key).get();
				}
			} catch (Exception e) {
				LOGGER.info("FAILURE server={}",clientEntry.getKey(), e);
			}
		}
	}
	
	@Override
	public void close() throws Exception {
		clients.entrySet().parallelStream().forEach(entry -> entry.getValue().shutdown(3,TimeUnit.SECONDS));
	}

	@Component
	@ConfigurationProperties
	public static class MemcachePingConfig {
		@Valid
		@NotNull
		Memcache memcache;

		public Memcache getMemcache() {
			return memcache;
		}

		public void setMemcache(Memcache memcache) {
			this.memcache = memcache;
		}

		public static class Memcache {
			@NotEmpty
			List<String> servers;
			@NotEmpty
			String username;
			@NotEmpty
			String password;

			public List<String> getServers() {
				return servers;
			}

			public void setServers(List<String> servers) {
				this.servers = servers;
			}

			public String getUsername() {
				return username;
			}

			public void setUsername(String username) {
				this.username = username;
			}

			public String getPassword() {
				return password;
			}

			public void setPassword(String password) {
				this.password = password;
			}
		}
	}
}
