package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.DB;
import db.DbException;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class SellerDaoJDBC implements SellerDao {

	private Connection conn;
	
	public SellerDaoJDBC(Connection conn) {
		this.conn = conn;
	}
	
	@Override
	public void insert(Seller obj) {
		PreparedStatement prepst = null;
		try {
			prepst = conn.prepareStatement(
					"INSERT INTO seller "
					+ "(Name, Email, BirthDate, BaseSalary, DepartmentId) "
					+ "VALUES "
					+ "(?, ?, ?, ?, ?) ",
					Statement.RETURN_GENERATED_KEYS);
			
			prepst.setString(1, obj.getName());
			prepst.setString(2, obj.getEmail());
			prepst.setDate(3,  new java.sql.Date(obj.getBirthdate().getTime()));
			prepst.setDouble(4, obj.getBaseSalary());
			prepst.setInt(5, obj.getDepartment().getId());
			
			int rowsAffected = prepst.executeUpdate();
			
			if(rowsAffected > 0) {
				ResultSet rs = prepst.getGeneratedKeys();
				if(rs.next()) {
					int id = rs.getInt(1);
					obj.setId(id);
				}
				DB.closeResultSet(rs);
			}
			else {
				throw new DbException("Unexpected error! No rows affected!");
			}
		}
		catch(SQLException e){
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(prepst);
		}
	}

	@Override
	public void update(Seller obj) {
		PreparedStatement prepst = null;
		try {
			prepst = conn.prepareStatement(
					"UPDATE seller "
				+ "SET Name = ?, Email = ?, BirthDate = ?, BaseSalary = ?, DepartmentId = ? "
				+ "WHERE Id = ? ");
				
			prepst.setString(1, obj.getName());
			prepst.setString(2, obj.getEmail());
			prepst.setDate(3,  new java.sql.Date(obj.getBirthdate().getTime()));
			prepst.setDouble(4, obj.getBaseSalary());
			prepst.setInt(5, obj.getDepartment().getId());
			prepst.setInt(6, obj.getId());
			
			prepst.executeUpdate();
		}
		catch(SQLException e){
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(prepst);
		}
}

	@Override
	public void deleteById(Integer id) {
		PreparedStatement prepst = null;
		try {
			prepst = conn.prepareStatement("DELETE FROM seller	WHERE Id = ?");
			prepst.setInt(1, id);
			prepst.executeUpdate();
		}
		catch(SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(prepst);
		}
}

	@Override
	public Seller findById(Integer id) {
		PreparedStatement prepst = null;
		ResultSet rs = null;
		try {
			prepst = conn.prepareStatement(
					"SELECT seller.*,department.Name as DepName "
					+ "FROM seller INNER JOIN department "
					+ "ON seller.DepartmentId = department.Id "
					+ "WHERE seller.Id = ?");
			
			prepst.setInt(1, id);
			rs = prepst.executeQuery();
			if(rs.next()) {
				Department dep = instantiateDepartment(rs);
				Seller obj = instantiateSeller(rs, dep);
				return obj;
			}
			return null;
		}
		catch(SQLException e){
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(prepst);
			DB.closeResultSet(rs);
		}
	}

	private Seller instantiateSeller(ResultSet rs, Department dep) throws SQLException {
		Seller obj = new Seller();
		obj.setId(rs.getInt("Id"));
		obj.setName(rs.getString("Name"));
		obj.setEmail(rs.getString("Email"));
		obj.setBaseSalary(rs.getDouble("BaseSalary"));
		obj.setBirthdate(rs.getDate("BirthDate"));
		obj.setDepartment(dep);
		return obj;
	}

	private Department instantiateDepartment(ResultSet rs) throws SQLException {
		Department dep = new Department();
		dep.setId(rs.getInt("DepartmentId"));
		dep.setName(rs.getString("DepName"));
		return dep;
	}

	@Override
	public List<Seller> findAll() {
		PreparedStatement prepst = null;
		ResultSet rs = null;
		try {
			prepst = conn.prepareStatement(
					"SELECT seller.*,department.Name as DepName "
					+ "FROM seller INNER JOIN department "
					+ "ON seller.DepartmentId = department.Id "
					+ "ORDER BY Name");
			
			rs = prepst.executeQuery();
			
			List<Seller> list = new ArrayList<>();
			//Map vazio para guardar qqr dpto. que for instanciar
			Map<Integer, Department> map = new HashMap<>();
			
			while(rs.next()) {
				
				Department dep = map.get(rs.getInt("DepartmentId"));
				
				if(dep == null) {
					dep = instantiateDepartment(rs);
					map.put(rs.getInt("DepartmentId"), dep);
				}
				
				Seller obj = instantiateSeller(rs, dep);
				list.add(obj);
			}
			return list;
		}
		catch(SQLException e){
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(prepst);
			DB.closeResultSet(rs);
		}
	}

	@Override
	public List<Seller> findByDepartment(Department department) {
		PreparedStatement prepst = null;
		ResultSet rs = null;
		try {
			prepst = conn.prepareStatement(
					"SELECT seller.*,department.Name as DepName "
					+ "FROM seller INNER JOIN department "
					+ "ON seller.DepartmentId = department.Id "
					+ "WHERE DepartmentId = ? "
					+ "ORDER BY Name");
			
			prepst.setInt(1, department.getId());
			
			rs = prepst.executeQuery();
			
			List<Seller> list = new ArrayList<>();
			//Map vazio para guardar qqr dpto. que for instanciar
			Map<Integer, Department> map = new HashMap<>();
			
			while(rs.next()) {
				
				Department dep = map.get(rs.getInt("DepartmentId"));
				
				if(dep == null) {
					dep = instantiateDepartment(rs);
					map.put(rs.getInt("DepartmentId"), dep);
				}
				
				Seller obj = instantiateSeller(rs, dep);
				list.add(obj);
			}
			return list;
		}
		catch(SQLException e){
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(prepst);
			DB.closeResultSet(rs);
		}
	}
}
