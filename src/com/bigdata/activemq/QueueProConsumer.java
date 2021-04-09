package com.bigdata.activemq;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;

/**
 * 
 * 点对点模式 一个生成者产生一个消息 只能被被一个消费者消费，消费完，消息就没有了
 */
public class QueueProConsumer {

	static ConnectionFactory connectionFactory=null;
	static {
		// 1.创建连接工厂
		connectionFactory = new ActiveMQConnectionFactory(
				ActiveMQConnection.DEFAULT_USER,
				ActiveMQConnection.DEFAULT_PASSWORD,
				"tcp://127.0.0.1:61616");
	}
	// 生产者
	public void queueProducer(U u) {
		MessageProducer producer = null;
		Session session = null;
		Connection connection = null;
		try {
			// 2.获取连接
			connection = connectionFactory.createConnection();
			// 3.启动连接
			connection.start();
			/*
			 * 4.获取session (参数1：是否启动事务, 
			 *               参数2：消息确认模式[ 
			 *               签收就是消费者接受到消息后，需要告诉消息服务器，我收到消息了。
			 *               当消息服务器收到回执后，本条消息将失效。
			 *               如果消费者收到消息后，并不签收，那么本条消息继续有效，很可能会被其他消费者消费掉
			 *               
			 *               AUTO_ACKNOWLEDGE = 1 自动确认
							 CLIENT_ACKNOWLEDGE = 2 客户端手动确认
							 DUPS_OK_ACKNOWLEDGE = 3 允许重复确认(签不签收无所谓了，只要消费者能够容忍重复的消息接受)
							 SESSION_TRANSACTED = 0 事务提交并确认 ])
			 */
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			// 5.创建队列对象
			Queue queue = session.createQueue("test-queue");
			// 6.创建消息生产者
			producer = session.createProducer(queue);
			// 消息持久化
			//producer.setDeliveryMode(DeliveryMode.PERSISTENT);
			// 7.创建消息
			//TextMessage textMessage = session.createTextMessage("欢迎来到MQ世界哈哈");
			ObjectMessage objectMessage=session.createObjectMessage();
			//定时发送  每个小时 定时延迟1秒后 每间隔1秒发送1次，总共发送10次 (需要在activemq.xml 中配置 schedulerSupport="true")
			/*objectMessage.setStringProperty(ScheduledMessage.AMQ_SCHEDULED_CRON, "0 * * * *");  
			objectMessage.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, 1000);
			objectMessage.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD, 1000);  
			objectMessage.setIntProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT, 10);*/
			
			HashMap<String,String> map=new HashMap<String, String>();
            map.put("param1", "姓名2");
            map.put("param2", "年龄2");
            map.put("param3", "性别2");
            map.put("user", u.toString());
			objectMessage.setObject(map);
            //objectMessage.setObject(u);
			// 8.发送消息
			producer.send(objectMessage);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// 9.关闭资源
			close(producer, session, connection,null);
		}
	}

	// 创建消费者
	public void queueConsumer() {
		Connection connection=null;
		Session session=null;
		MessageConsumer consumer=null;
		Queue queue=null;
		try {
			//使所有的类都能够被传输
			((ActiveMQConnectionFactory) connectionFactory).setTrustAllPackages(true);
			//使指定的包里的类能够被传输
			//((ActiveMQConnectionFactory) connectionFactory).setTrustedPackages(new ArrayList(Arrays.asList("com.bigdata.activemq,com.bigdata.hadoop".split(","))));
			// 2.获取连接
			connection = connectionFactory.createConnection();
			// 3.启动连接
			connection.start();
			// 4.获取session (参数1：是否启动事务,参数2：消息确认模式)
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			// 5.创建队列对象
			queue = session.createQueue("test-queue");
			// 6.创建消息消费者
			consumer = session.createConsumer(queue);
			// 7.监听消息
			consumer.setMessageListener(new MyMessageListener(consumer));
			// 8.等待键盘输入
			System.in.read();
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			// 9.关闭资源
			close(null, session, connection,consumer);
		}
	}
    public MessageConsumer getConsumer(){
    	Connection connection=null;
		Session session=null;
		MessageConsumer consumer=null;
		Queue queue=null;
		try {
			//使所有的类都能够被传输
			((ActiveMQConnectionFactory) connectionFactory).setTrustAllPackages(true);
			//使指定的包里的类能够被传输
			//((ActiveMQConnectionFactory) connectionFactory).setTrustedPackages(new ArrayList(Arrays.asList("com.bigdata.activemq,com.bigdata.hadoop".split(","))));
			// 2.获取连接
			connection = connectionFactory.createConnection();
			// 3.启动连接
			connection.start();
			// 4.获取session (参数1：是否启动事务,参数2：消息确认模式)
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			// 5.创建队列对象
			queue = session.createQueue("test-queue");
			// 6.创建消息消费者
			consumer = session.createConsumer(queue);
			// 7.监听消息
			//consumer.setMessageListener(new MyMessageListener(consumer));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return consumer;
    }
	public void close(MessageProducer producer, Session session, Connection connection,MessageConsumer consumer) {
		try {
			if (producer != null) {
				producer.close();
			}
			if(consumer!=null){
				consumer.close();
			}
			if (session != null) {
				session.close();
			}
			if (connection != null) {
				connection.close();
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	// 定时执行生产 设定指定任务task在指定延迟delay后进行固定延迟peroid的执行
	int k=0;
	public  void shendtimer() {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.HOUR_OF_DAY, 9); // 控制时
		calendar.set(Calendar.MINUTE, 0); // 控制分
		calendar.set(Calendar.SECOND, 0); // 控制秒
		Date time = calendar.getTime();  // 得出执行任务的时间
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
			public void run() {
				System.out.println("开始生产...");
				//调用生产者
				U u=new U();
				u.setCode(k++);
				u.setText("文本"+k);
				queueProducer(u);
			}
		}, 2000, 1000);
	}
	
	
	/**
	 * 获取消息队列状态 
	 * 
	 * 实现监控必须添加如下配置
	 * 1.在你下载activemq文件夹下的config中，找到activemq.xml，在broker节点增加useJmx=”true”
         useJmx表示开启jmx监控
         <broker xmlns="http://activemq.apache.org/schema/core" brokerName="localhost" dataDirectory="${activemq.data}" schedulerSupport="true" useJmx="true">
       2.在activemq.xml中找到managementContext节点并更改
         <managementContext>
		     <managementContext createConnector="true" connectorPort="11099"/>
		 </managementContext>
	   3.在bin目录下找到activemq文件，在文件最后一行添加如下信息
	     Windows系统
	     SUNJMX=-Dcom.sun.management.jmxremote.port=1616 -Dcom.sun.management.jmxremote.ssl=false
                -Dcom.sun.management.jmxremote.password.file=%ACTIVEMQ_BASE%/conf/jmx.password
                -Dcom.sun.management.jmxremote.access.file=%ACTIVEMQ_BASE%/conf/jmx.access
         linux系统
         SUNJMX="-Dcom.sun.management.jmxremote.port=1616 -Dcom.sun.management.jmxremote.ssl=false
                 -Dcom.sun.management.jmxremote.password.file=${ACTIVEMQ_BASE}/conf/jmx.password
                 -Dcom.sun.management.jmxremote.access.file=${ACTIVEMQ_BASE}/conf/jmx.access"
	 */
	int num=1;
	public int getMqState(MessageConsumer consumer){

		String urll="service:jmx:rmi:///jndi/rmi://localhost:11099/jmxrmi";
		String broker="org.apache.activemq:brokerName=localhost,type=Broker";
		try {
			JMXServiceURL url = new JMXServiceURL(urll);  
	        JMXConnector connector = JMXConnectorFactory.connect(url, null);
	        connector.connect();  
	        MBeanServerConnection connection = connector.getMBeanServerConnection();  
	        
	        ObjectName name = new ObjectName(broker);  
	        BrokerViewMBean mBean =  (BrokerViewMBean)MBeanServerInvocationHandler.newProxyInstance(connection,name, BrokerViewMBean.class, true);  
	        // System.out.println(mBean.getBrokerName());
	        for(ObjectName queueName : mBean.getQueues()) {  
	            QueueViewMBean queueMBean = (QueueViewMBean)MBeanServerInvocationHandler.newProxyInstance(connection, queueName, QueueViewMBean.class, true);  
	            System.out.println("\n------------------------------\n");  
	            // 消息队列名称  
	            System.out.println("消息队列名称--- " + queueMBean.getName());  
	            // 队列中剩余的消息数  
	            System.out.println("队列中剩余的消息数 --- " + queueMBean.getQueueSize());  
	            // 消费者数  
	            System.out.println("消费者数 --- " + queueMBean.getConsumerCount());  
	            // 出队数  
	            System.out.println("出队数 ---" + queueMBean.getDequeueCount()); 
	            
	            if("test-queue".equals(queueMBean.getName())){
	            	num++;
	            }
	        } 
	       
		} catch (Exception e) {
			e.printStackTrace();
		}
		return num;
	}
	
	public static void main(String[] args) {
		QueueProConsumer prodconsumer=new QueueProConsumer();
		
		//调用生产者
		//prodconsumer.queueProducer(new U());
		//定时生产
		//prodconsumer.shendtimer();
		//调用消费者
		prodconsumer.queueConsumer();
		//查看队列消息
		MessageConsumer consumer= prodconsumer.getConsumer();
		prodconsumer.getMqState(consumer);
	}
	
	class MyMessageListener  implements MessageListener{
		
		MessageConsumer consumer;
		public MyMessageListener(MessageConsumer target){
			consumer=target;
		}

		@Override
		public void onMessage(Message message) {
			//TextMessage textMessage = (TextMessage) message;
			ObjectMessage objectMessage=(ObjectMessage)message;
			try {
                int num=getMqState(consumer);
                if(num>2){
            		//consumer.close();
            	}
				//System.out.println("接收到消息:" + textMessage.getText());
				System.out.println("接收到对象消息:" + objectMessage.getObject());
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
}
