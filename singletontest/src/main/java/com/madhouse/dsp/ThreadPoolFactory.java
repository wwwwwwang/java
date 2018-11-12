package com.madhouse.dsp;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Created by Madhouse on 2017/9/21.
 */
public class ThreadPoolFactory {
    private static final Map<String, ExecutorService> poolMap = new HashMap<String, ExecutorService>();
    private static class ThreadPollFactorySingleton{
        private static ThreadPoolFactory instance = new ThreadPoolFactory();
    }
    private ThreadPoolFactory(){}
    public static ThreadPoolFactory getInstance(){
        return ThreadPollFactorySingleton.instance;
    }

    final ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat("Orders-%d")
            .setDaemon(false)
            .build();

    private ExecutorService createPool(int num){
        //return Executors.newFixedThreadPool(num);
        return Executors.newFixedThreadPool(num, threadFactory);

        //ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
        //ExecutorService fixedThreadPool = Executors.newFixedThreadPool(3);
        //ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
        //ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(5);
    }

    public ExecutorService getPool(String key){
        return getPool(key, 2);
    }

    public ExecutorService getPool(String key, int num){
        if(poolMap.containsKey(key)){
            return poolMap.get(key);
        }else{
            ExecutorService pool = createPool(num);
            poolMap.put(key,pool);
            return pool;
        }
    }
}
