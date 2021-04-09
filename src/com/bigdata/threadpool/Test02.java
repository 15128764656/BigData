package com.bigdata.threadpool;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
/**
 * 
    newCachedThreadPool() : 创建一个大小无界的缓冲线程池，它的任务队列是一个同步队列，任务加入加入到池中，
                                                                                   如果池中有空闲线程， 则用空闲线程执行，如无则创建新线程执行，
                                                                                   池中的空闲线程超过60秒， 将被销毁释放， 线程数随任务的多少变化，适用于耗时较小的异步任务， 
                                                                                   池的核心线程数=0， 最大线程数=Integer.MAX_VALUE
                                                                                   
       https://blog.csdn.net/qq_38428623/article/details/86688783
 */

public class Test02 {
	// 线程数
    private static final int threads = 20;
    // 用于计数线程是否执行完成
    CountDownLatch countDownLatch = new CountDownLatch(threads);
    
	public void test01(){
		ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
		
		//执行任务次数
		for(int i=0;i<threads;i++){
			cachedThreadPool.execute(()-> {
				try {
					String name=Thread.currentThread().getName();
					System.out.println("线程名称=="+name);
				} catch (Exception e) {
					e.printStackTrace();
				}finally{
					countDownLatch.countDown();
				}
			});
		}
		try {
			countDownLatch.await();
			System.out.println(countDownLatch.getCount());
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) {
		new Test02().test01();
	}
}



