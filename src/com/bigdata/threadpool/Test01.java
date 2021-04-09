package com.bigdata.threadpool;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
/**
 * corePoolSize         核心线程数
 * maximumPoolSize      最大线程数
 * keepAliveTime        线程存活时间
 * unit                 时间单位
 * 
	    TimeUnit.DAYS;              //天
		TimeUnit.HOURS;             //小时
		TimeUnit.MINUTES;           //分钟
		TimeUnit.SECONDS;           //秒
		TimeUnit.MILLISECONDS;      //毫秒
		TimeUnit.MICROSECONDS;      //微妙
		TimeUnit.NANOSECONDS;       //纳秒
    
 * workQueue            任务队列 一般由下面3种类型
 * 
 * 1)ArrayBlockingQueue：基于数组的先进先出队列，此队列创建时必须指定大小；
　　 2)LinkedBlockingQueue：基于链表的先进先出队列，如果创建时没有指定此队列大小，则默认为Integer.MAX_VALUE；
　　 3)synchronousQueue：这个队列比较特殊，它不会保存提交的任务，而是将直接新建一个线程来执行新来的任务
 * 
 * 假如有一个工厂，工厂里面有10个工人，每个工人同时只能做一件任务。
         因此只要当10个工人中有工人是空闲的，来了任务就分配给空闲的工人做；
         当10个工人都有任务在做时，如果还来了任务，就把任务进行排队等待；

　　如果说新任务数目增长的速度远远大于工人做任务的速度，那么此时工厂主管可能会想补救措施，比如重新招4个临时工人进来；
        然后就将任务也分配给这4个临时工人做；

　　如果说着14个工人做任务的速度还是不够，此时工厂主管可能就要考虑不再接收新的任务或者抛弃前面的一些任务了。
　　当这14个工人当中有人空闲时，而新任务增长的速度又比较缓慢，工厂主管可能就考虑辞掉4个临时工了，只保持原来的10个工人，毕竟请额外的工人是要花钱的


        这个例子中的corePoolSize就是10，而maximumPoolSize就是14（10+4）
　　也就是说corePoolSize就是线程池大小，maximumPoolSize在我看来是线程池的一种补救措施，即任务量突然过大时的一种补救措施
 *
 * 当拒绝处理任务时的策略，有以下四种取值
 *  ThreadPoolExecutor.AbortPolicy:丢弃任务并抛出RejectedExecutionException异常。 
	ThreadPoolExecutor.DiscardPolicy：也是丢弃任务，但是不抛出异常。 
	ThreadPoolExecutor.DiscardOldestPolicy：丢弃队列最前面的任务，然后重新尝试执行任务（重复此过程）
	ThreadPoolExecutor.CallerRunsPolicy：重试添加当前的任务，他会自动重复调用execute()方法 
 *
 *  注意：
 *  在java doc中，并不提倡我们直接使用ThreadPoolExecutor，而是使用Executors类中提供的几个静态方法来创建线程池：

	newFixedThreadPool(int nThreads) : 创建一个固定大小，任务队列无界的的线程池，核心线程数=最大线程数
    newCachedThreadPool() : 创建一个大小无界的缓冲线程池，它的任务队列是一个同步队列，任务加入加入到池中，
                                                                                   如果池中有空闲线程， 则用空闲线程执行，如无则创建新线程执行，
                                                                                   池中的空闲线程超过60秒， 将被销毁释放， 线程数随任务的多少变化，适用于耗时较小的异步任务， 
                                                                                   池的核心线程数=0， 最大线程数=Integer.MAX_VALUE
    newSingleThreadExecutor() : 只有一个线程来执行无界任务队列的单一线程池，该线程池确保任务按加入的顺序一个一个的依次执行，
                                                                                                当唯一的线程因任务异常中止时，将创建一个新的线程来继续执行后续的任务，它只会用唯一的工作线程来执行任务，保证所有任务按照指定顺序(FIFO, LIFO, 优先级)执行
                                                                                                与newFixedThreadPool(1)的区别在于，单一线程池的池大小在newSingleThreadExecutor方法中硬编码，不能再改变
    newScheduledThreadPool(int corePoolSize) : 能定时执行任务的线程池，该池的核心线程数由参数指定，最大线程数=Integer.MAX_VALUE
 */

public class Test01 {
	
	public static void main(String[] args) {
		//创建线程池
		/*ThreadPoolExecutor  executor =new ThreadPoolExecutor(
				5, 10, 200, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(5),new ThreadPoolExecutor.CallerRunsPolicy());*/
		
		ThreadPoolExecutor  executor =new ThreadPoolExecutor(5, 10, 1000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(5), new RejectedExecutionHandler() {
			@Override
			public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
				System.err.println("有任务被拒绝执行了");
				if (!executor.isShutdown()) {
	                r.run();
	            }
			}
		});
		
		//执行15个任务
		for(int i =1;i<=26;i++){
			final int  index=i;
			MyTarsk tarsk=new MyTarsk(index);
			executor.execute(tarsk);
			
			/*MyTarskCall tarsk=new MyTarskCall(index);
			Future<String> submit = executor.submit(tarsk);*/
			
			System.out.println("线程池中的数量=="+executor.getPoolSize()+","
					+ "队列中等待执行的任务数目=="+executor.getQueue().size()+","
					+ "已执行完毕任务=="+executor.getCompletedTaskCount());
			try {
				//System.out.println(submit.get());
			}  catch (Exception e) {
				e.printStackTrace();
			}
		}
		executor.shutdown();
		
	}
}
class MyTarsk implements Runnable{
	private int num;
	
	public MyTarsk(int index){
		this.num=index;
	}
	
	@Override
	public void run() {
		try {
			System.out.println(Thread.currentThread().getName()+"正在执行任务"+num);
            Thread.currentThread().sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("任务 "+num+"执行完毕");
	}
}

/**
 * 线程是属于异步计算模型，所以你不可能直接从别的线程中得到函数返回值。
        这时候，Future就出场了。Futrue可以监视目标线程调用call的情况，
        当你调用Future的get()方法以获得结果时，当前线程就开始阻塞，直接call方法结束返回结果
 *
 */
class MyTarskCall implements Callable<String>{
    private int num;
	
	public MyTarskCall(int index){
		this.num=index;
	}
	@Override
	public String call() throws Exception {
		System.out.println("正在执行任务"+num);
		String tname=Thread.currentThread().getName();
		try {
            Thread.currentThread().sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("任务 "+num+"执行完毕");
		return tname;
	}
}
