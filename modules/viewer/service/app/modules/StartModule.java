package modules;

import com.google.inject.AbstractModule;
import org.sunbird.cache.util.RedisCacheUtil;
import org.sunbird.common.ProjectUtil;

/**
 * This class is responsible for creating instance of
 * ApplicationStart at server startup time.
 *
 * @author Jaikumar Soundara Rajan
 */
public class StartModule extends AbstractModule {
    @Override
    protected void configure() {
        System.out.println("StartModule:configure: Start");
        try {
            bind(ApplicationStart.class).asEagerSingleton();
            if (Boolean.parseBoolean(ProjectUtil.getConfigValue("redis.enabled"))) {
                bind(RedisCacheUtil.class).asEagerSingleton();
            }
        } catch (Exception | Error e) {
            e.printStackTrace();
            throw e;
        }
        System.out.println("StartModule:configure: End");

    }
}
