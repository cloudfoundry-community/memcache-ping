package cloudfoundry.memcache.ping;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang.StringUtils;
import org.hibernate.validator.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.auth.AuthInfo;
import net.rubyeye.xmemcached.command.BinaryCommandFactory;
import net.rubyeye.xmemcached.utils.AddrUtil;

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
			client.set(key, 5, key);
			try {
				long time = System.nanoTime();
				if(key.equals(client.get(key))) {
					long total = System.nanoTime()-time;
					LOGGER.info("Success time="+total+"ns");
				}
			} finally {
				client.delete(key);
			}
		} catch(Exception e) {
			LOGGER.info("Failure "+e.getMessage());
		}
	}
	
	@Bean
	public MemcachedClient memcache(MemcachePingConfig config) throws Exception {

		List<InetSocketAddress> addresses = AddrUtil.getAddresses(StringUtils.join(config.getMemcache().getServers(), ' '));
		XMemcachedClientBuilder builder = new XMemcachedClientBuilder(addresses);
		Map<InetSocketAddress, AuthInfo> authInfo = new HashMap<>();
		for (InetSocketAddress address : addresses) {
			authInfo.put(address, AuthInfo.plain(config.getMemcache().getUsername(), config.getMemcache().getPassword()));
		}
		builder.setAuthInfoMap(authInfo);
		builder.setCommandFactory(new BinaryCommandFactory());
		builder.setConnectTimeout(10000);
		builder.setOpTimeout(10000);
		builder.setEnableHealSession(true);
		builder.setFailureMode(false);
		MemcachedClient client = builder.build();
		client.setEnableHeartBeat(true);

		return client;
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
