package cloudfoundry.memcache.ping;

import java.util.List;
import java.util.UUID;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang.StringUtils;
import org.hibernate.validator.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.ConnectionFactoryBuilder.Protocol;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.auth.PlainCallbackHandler;

@SpringBootApplication
@EnableScheduling
public class MemcachePingApplication {
	private static final Logger LOGGER = LoggerFactory.getLogger(MemcachePingApplication.class);
	
	@Autowired
	MemcachedClient client;

	public static void main(String[] args) {
		SpringApplication.run(MemcachePingApplication.class, args);
		LOGGER.info("Memcache Ping Started.");
	}
	
	@Scheduled(fixedDelayString="${pingInterval}")
	public void scheduledTask() {
		String key = UUID.randomUUID().toString();
		try {
			client.set(key, 5, key).get();
			try {
				long time = System.nanoTime();
				if(key.equals(client.get(key))) {
					long total = System.nanoTime()-time;
					LOGGER.info("SUCCESS time_ns="+total);
				} else {
					throw new IllegalStateException("Value returned didn't match expected value.  WHAT IS GOING ON!");
				}
			} finally {
				client.delete(key).get();
			}
		} catch(Exception e) {
			LOGGER.info("FAILURE msg="+e.getMessage());
		}
	}
	
	@Bean
	public MemcachedClient memcache(MemcachePingConfig config) throws Exception {

		ConnectionFactoryBuilder binaryConnectionFactory = new ConnectionFactoryBuilder();
		binaryConnectionFactory.setProtocol(Protocol.BINARY);
		binaryConnectionFactory.setAuthDescriptor(new AuthDescriptor(new String[] {"PLAIN"}, new PlainCallbackHandler(config.getMemcache().getUsername(), config.getMemcache().getPassword())));
		binaryConnectionFactory.setShouldOptimize(false);

		return new net.spy.memcached.MemcachedClient(binaryConnectionFactory.build(), AddrUtil.getAddresses(StringUtils.join(config.getMemcache().getServers(), ' ')));
	}
	
	@Component
	@ConfigurationProperties
	public static class MemcachePingConfig {
		@Valid @NotNull Memcache memcache;

		public Memcache getMemcache() {
			return memcache;
		}
		public void setMemcache(Memcache memcache) {
			this.memcache = memcache;
		}

		public static class Memcache {
			@NotEmpty List<String> servers;
			@NotEmpty String username;
			@NotEmpty String password;

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
