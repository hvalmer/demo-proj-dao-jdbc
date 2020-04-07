package application;

import model.dao.DaoFactory;
import model.dao.SellerDao;
import model.entities.Seller;

public class ProgramDemoProj {

	public static void main(String[] args) {
	
		SellerDao sellerDao = DaoFactory.createSellerDao();
	
		System.out.println("=== TEST nº 1: seller findById ===");
		Seller seller = sellerDao.findById(3);
		System.out.println(seller);
	}
}
