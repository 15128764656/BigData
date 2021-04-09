package com.bigdata.thread;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 学生持久层类
 * @author Administrator
 * 22654 22229
 */
public class StuDao {
	int k=0;
	public static void main(String[] args) {
		StuDao studao=new StuDao();
		//查询
		Student queryst=new Student();
		//queryst.setSname("超");
		/*queryst.setSex("0");
		List<Student> list=studao.query(queryst);
		for(Student st:list){
			System.err.println(st);
		}*/
		
		//增加
		Student addstu=new Student();
		addstu.setSex("0");
		addstu.setSage(20);
		addstu.setIndate(getSysDate());
		addstu.setSalary(new BigDecimal("2000.98"));
		addstu.setSname("学生");
		//int k=studao.add(addstu);
		
		//int k=studao.addBatch(addstu);
		
		//删除单条
		//int k=studao.del("475002");
		//批量删除
		/*int k=studao.delMore("'475003','475004','475005'");
		System.err.println(k>0?"增加成功":"增加失败");*/
		
		//通过ID查询待修改的数据
		//System.err.println(studao.getObjById("475006"));
		
		//修改
		Student updatestu=new Student();
		updatestu.setSname("冯德智");
		updatestu.setIndate(getSysDate());
		/*updatestu.setSage(20);
		updatestu.setSalary(new BigDecimal("2000.09"));
		updatestu.setSex("1");
		updatestu.setGrade(new Grade("10","java2008"));*/
		updatestu.setSnum("475006");
		//int k=studao.update(updatestu);
		//System.err.println(k>0?"修改成功":"增加失败");
		
		//分页查询
		Map<String,String> map=new HashMap<String,String>();
		String currentPage="1";
		String pageSize="100";
		map.put("currentPage",currentPage);
		map.put("pageSize", pageSize);
		//查询条件
		map.put("sname", "学生");
		map.put("sex", "0");
		
		List<Student> list=studao.queryPage(map);
		//总额记录数
		int count=studao.queryCont(map);
		//总页数
		int pageSum=count%Integer.parseInt(pageSize)==0?count/Integer.parseInt(pageSize):count/Integer.parseInt(pageSize)+1;
		System.err.println("当前第"+currentPage+"页,每页"+pageSize+",总记录数"+count+",总页数"+pageSum);
		for(Student st:list){
			System.err.println(st);
		}
	}
	
	
	public static Date getSysDate(){
		java.util.Date date=new java.util.Date();
		Date sqlDate=new Date(date.getTime());
		return sqlDate;
	}
	
	public int add(Student stu){
		//获取连接对象
		Connection con = DbUtil.getCon();
		
		//通过sql语句执行器,预编译sql
		PreparedStatement ps=null;
		int n=0;
		if(stu==null){
			return n;
		}
		try {
			//con.setAutoCommit(false);
			
			String sql=" insert into t_stu (snum,sname,sage,indate,salary,sex,gid) "
					+ "  values(test_seq.nextval,?,?,?,?,?,?)";
			ps=con.prepareStatement(sql);
			//给？ 占位符赋值
			
			ps.setString(1, stu.getSname());
			ps.setInt(2, stu.getSage());
			ps.setDate(3, stu.getIndate());
			ps.setBigDecimal(4, stu.getSalary());
			ps.setString(5, stu.getSex());
			ps.setString(6, stu.getGrade()==null?"":stu.getGrade().getGid());
			
			n=ps.executeUpdate();
			
			//con.commit();
			
		} catch (Exception e) {
			e.printStackTrace();
			//con.rollback();
		}finally{
			DbUtil.close(ps,null,con);
		}
		return n;
	}
	
	//批量插入
	public int addBatch(Student stu){
		//获取连接对象
		Connection con = DbUtil.getCon();
		
		//通过sql语句执行器,预编译sql
		PreparedStatement ps=null;
		int n=0;
		if(stu==null){
			return n;
		}
		try {
			con.setAutoCommit(false);
			
			String sql=" insert into t_stu (snum,sname,sage,indate,salary,sex,gid) "
					+ "  values(test_seq.nextval,?,?,?,?,?,?)";
			ps=con.prepareStatement(sql);
			long start=System.currentTimeMillis();
			//给？ 占位符赋值
			for(int i=0;i<10000000;i++){
				ps.setString(1, stu.getSname()+i);
				ps.setInt(2, stu.getSage());
				ps.setDate(3, stu.getIndate());
				ps.setBigDecimal(4, stu.getSalary());
				ps.setString(5, stu.getSex());
				ps.setString(6, stu.getGrade()==null?"":stu.getGrade().getGid());
				ps.addBatch();
				if(i%1000==0){
					ps.executeBatch();
					con.commit();
					ps.clearBatch();
				}
			}
			ps.executeBatch();
			con.commit();
			ps.clearBatch();
			long end=System.currentTimeMillis();
			System.err.println("用时=="+(end-start)/1000f+"秒");
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			//DbUtil.close(ps,null,con);
		}
		return n;
	}
	
	
	public List<Student> query(Student st){
		//获取连接对象
		Connection con = DbUtil.getCon();
		//通过sql语句执行器,预编译sql
		PreparedStatement ps=null;
		ResultSet rs=null;
		List<Student> list=new ArrayList<Student>();
		try {
			String sql="select t.snum,t.sname,t.sage,t.indate,t.salary,decode(t.sex,'0','女','1','男','其它') sex,"
					+ " t.gid,g.gname "
					+ " from t_stu t left join t_grade g on t.gid=g.gid where 1=1 ";
			
			if(st.getSname()!=null && !st.getSname().equals("")){
				sql+=" and t.sname like ? ";
			}
			if(st.getSex()!=null && !st.getSex().equals("")){
				sql+=" and t.sex=? ";
			}
			ps = con.prepareStatement(sql);
			//给？号占位符赋值
			int i=1;
			if(st.getSname()!=null && !st.getSname().equals("")){
				ps.setString(i++, "%"+st.getSname()+"%");
			}
			if(st.getSex()!=null && !st.getSex().equals("")){
				ps.setString(i++, st.getSex());
			}
			//执行sql 语句，查询数据
			rs=ps.executeQuery();
			//遍历结果集
			while(rs.next()){
				int sage=rs.getInt("sage");
				BigDecimal salary = rs.getBigDecimal("salary");
				String sex=rs.getString("sex");
				String gid=rs.getString("gid");
				String gname=rs.getString("gname");
				Student stt=new Student();
				//将数据库中每一行记录，映射成一个student对象
				stt.setSnum(rs.getString(1));
				stt.setSname(rs.getString("sname"));
				stt.setIndate(rs.getDate("indate"));
				stt.setSex(sex);
				stt.setSalary(salary);
				stt.setGrade(new Grade(gid,gname));
				//将student对象放到list中
				list.add(stt);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			DbUtil.close(ps,rs,con);
		}
		return list;
	}
	
	/**
	 * 单条删除
	 * @param id
	 * @return
	 */
	public int del(String id){
		//获取连接对象
		Connection con = DbUtil.getCon();
		
		//通过sql语句执行器,预编译sql
		PreparedStatement ps=null;
		int n=0;
		try {
			//con.setAutoCommit(false);
			String sql=" delete from  t_stu t where t.snum=? ";
			ps=con.prepareStatement(sql);
			//给？ 占位符赋值
			ps.setString(1,id);
			n=ps.executeUpdate();
			//con.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			DbUtil.close(ps,null,con);
		}
		return n;
	}
	
	/**
	 * 批量删除
	 * @param id
	 * @return
	 */
	public int delMore(String ids){
		//获取连接对象
		Connection con = DbUtil.getCon();
		
		//通过sql语句执行器,预编译sql
		PreparedStatement ps=null;
		int n=0;
		try {
			//con.setAutoCommit(false);
			String sql=" delete from  t_stu t where t.snum in ("+ids+")";
			ps=con.prepareStatement(sql);
			n=ps.executeUpdate();
			//con.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			DbUtil.close(ps,null,con);
		}
		return n;
	}
	/**
	 * 通过ID查询待修改的数据
	 * @param id
	 * @return
	 */
	public Student getObjById(String id){
		//获取连接对象
		Connection con = DbUtil.getCon();
		//通过sql语句执行器,预编译sql
		PreparedStatement ps=null;
		ResultSet rs=null;
		Student stt=new Student();
		try {
			String sql="select t.* from t_user t where t.id=? ";
			
			ps = con.prepareStatement(sql);
			//给？号占位符赋值
			ps.setString(1, id);
			//执行sql 语句，查询数据
			rs=ps.executeQuery();
			//遍历结果集
			if(rs.next()){
				int sage=rs.getInt("age");
				BigDecimal salary = rs.getBigDecimal("salary");
				String sex=rs.getString("sex");
				String gname=rs.getString("name");
				//将数据库中每一行记录，映射成一个student对象
				stt.setSname(gname);
				stt.setSex(sex);
				stt.setSage(sage);
				stt.setSalary(salary);
			}
			System.out.println(stt);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("k==========="+k);
		}finally{
			DbUtil.close(ps,rs,con);
		}
		return stt;
	}
	
	/**
	 * 修改
	 * @param student
	 * @return
	 */
	public int update(Student student){
		//获取连接对象
		Connection con = DbUtil.getCon();
		//通过sql语句执行器,预编译sql
		PreparedStatement ps=null;
		int n=0;
		try {
			String sql=" update t_stu t set t.snum=t.snum";
			
			if(!isEmpty(student.getSname())){
				sql+=",t.sname=?";
			}
			if(student.getSage()>0){
				sql+=",t.sage=?";
			}
			if(!isEmpty(student.getIndate())){
				sql+=",t.indate=?";
			}
			if(!isEmpty(student.getSalary())){
				sql+=",t.salary=?";
			}
			if(!isEmpty(student.getSex())){
				sql+=",t.sex=?";
			}
			if(!isEmpty(student.getGrade())){
				if(!isEmpty(student.getGrade().getGid())){
					sql+=",t.gid=?";
				}
			}
			sql+=" where t.snum=? ";
			
			ps=con.prepareStatement(sql);
			//给 ？占位符赋值
			int i=1;
			if(!isEmpty(student.getSname())){
				ps.setString(i++, student.getSname());
			}
			if(student.getSage()>0){
				ps.setInt(i++, student.getSage());
			}
			if(!isEmpty(student.getIndate())){
				ps.setDate(i++, student.getIndate());
			}
			if(!isEmpty(student.getSalary())){
				ps.setBigDecimal(i++, student.getSalary());
			}
			if(!isEmpty(student.getSex())){
				ps.setString(i++, student.getSex());
			}
			if(!isEmpty(student.getGrade())){
				if(!isEmpty(student.getGrade().getGid())){
					ps.setString(i++,student.getGrade().getGid());
				}
			}
			ps.setString(i++, student.getSnum());
			
			n=ps.executeUpdate();
			//con.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			DbUtil.close(ps,null,con);
		}
		return n;
	}
    
	/**
	 * 分页查询
	 * @param obj
	 * @return
	 */
	public List<Student> queryPage(Map<String,String> qureyPram){
		//获取连接对象
		Connection con = DbUtil.getCon();
		//通过sql语句执行器,预编译sql
		PreparedStatement ps=null;
		ResultSet rs=null;
		List<Student> list=new ArrayList<Student>();
		//当前页数
		int currentPage=isEmpty(qureyPram.get("currentPage"))==true?1:Integer.parseInt(qureyPram.get("currentPage"));
		if(currentPage<=1){
			currentPage=1;
		}
		//每页记录数
		int pageSize=isEmpty(qureyPram.get("pageSize"))==true?10:Integer.parseInt(qureyPram.get("pageSize"));
		//起始行
		int startRow=(currentPage-1)*pageSize+1;
		//结束行
		int endRow=currentPage*pageSize;
		
		try {
			String sql="select b.* from ( "
					  + " select rownum rn,a.* from ( "
					  + "    select t.snum,t.sname,t.sage,t.indate,t.salary,decode(t.sex,'0','女','1','男','其它') sex,"
					  + "       t.gid,g.gname from t_stu t "
					  + "       left join t_grade g on t.gid=g.gid "
					  + "       where 1=1 ";
								if(isEmpty(qureyPram.get("sname"))){
									sql+=" and t.sname like ? ";
								}
								if(isEmpty(qureyPram.get("sex"))){
									sql+=" and t.sex=? ";
								}
					      sql+=") a where rownum<="+endRow+" "
					  	  + ")b where b.rn>="+startRow+" ";
			ps = con.prepareStatement(sql);
			//给？号占位符赋值
			int i=1;
			if(isEmpty(qureyPram.get("sname"))){
				ps.setString(i++, "%"+qureyPram.get("sname")+"%");
			}
			if(isEmpty(qureyPram.get("sex"))){
				ps.setString(i++, qureyPram.get("sex"));
			}
			
			//执行sql 语句，查询数据
			rs=ps.executeQuery();
			//遍历结果集
			while(rs.next()){
				int sage=rs.getInt("sage");
				BigDecimal salary = rs.getBigDecimal("salary");
				String sex=rs.getString("sex");
				String gid=rs.getString("gid");
				String gname=rs.getString("gname");
				Student stt=new Student();
				//将数据库中每一行记录，映射成一个student对象
				stt.setSnum(rs.getString(1));
				stt.setSname(rs.getString("sname"));
				stt.setIndate(rs.getDate("indate"));
				stt.setSex(sex);
				stt.setSalary(salary);
				stt.setGrade(new Grade(gid,gname));
				//将student对象放到list中
				list.add(stt);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			DbUtil.close(ps,rs,con);
		}
		return list;
	}
	
	/**
	 * 总记录数
	 * @param qureyPram
	 * @return
	 */
	public int queryCont(Map<String,String> qureyPram){
		//获取连接对象
		Connection con = DbUtil.getCon();
		//通过sql语句执行器,预编译sql
		PreparedStatement ps=null;
		ResultSet rs=null;
		int count=0;
		try {
			String sql="    select count(*) count from t_stu t "
				     + "    left join t_grade g on t.gid=g.gid "
				     + "    where 1=1 ";
							if(isEmpty(qureyPram.get("sname"))){
								sql+=" and t.sname like ? ";
							}
							if(isEmpty(qureyPram.get("sex"))){
								sql+=" and t.sex=? ";
							}
			ps = con.prepareStatement(sql);
			//给？号占位符赋值
			int i=1;
			if(isEmpty(qureyPram.get("sname"))){
				ps.setString(i++, "%"+qureyPram.get("sname")+"%");
			}
			if(isEmpty(qureyPram.get("sex"))){
				ps.setString(i++, qureyPram.get("sex"));
			}
			//执行sql 语句，查询数据
			rs=ps.executeQuery();
			//遍历结果集
			while(rs.next()){
				count= rs.getInt("count");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			DbUtil.close(ps,rs,con);
		}
		return count;
	}
	
	public boolean isEmpty(Object obj){
		if(obj instanceof String){
			String str=(String)obj;
			if(str==null || "".equals(str)){
				return true;
			}
		}else{
			if(obj==null){
				return true;
			}
		}
		return false;
	}
}
