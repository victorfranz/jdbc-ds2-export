package ds2.export

import java.sql.{ Connection, DriverManager, ResultSet }
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.util.Date
import java.util.GregorianCalendar
import java.util.Calendar
import faker._
import scalaj.http._
import scala.collection.mutable.ListBuffer
import java.io._
import java.net.URI
import java.text.SimpleDateFormat
import scala.concurrent._
import ExecutionContext.Implicits.global

/**
 * Read and export DS2 customers and orders in JSON Format
 * @author victor
 */
object JdbcExportOrders {

  def main(args: Array[String]) {
    generateUserDataFromFakeNameGenerator();
    generateOrders();
  }

  def generateOrders() {
    // Change to Your Database Config
    val conn_str = "jdbc:mysql://localhost:3306/DS2?user=root&password=root"
    // Setup the connection
    val conn = DriverManager.getConnection(conn_str)
    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      // Execute Query
      val rs = statement.executeQuery("SELECT ORDERID, ORDERDATE, CUSTOMERID, NETAMOUNT, TAX, TOTALAMOUNT FROM ORDERS ")
      val selectOrderProduct = conn.prepareStatement("SELECT PROD_ID, QUANTITY FROM ORDERLINES WHERE ORDERID = ? ")
      val selectProduct = conn.prepareStatement("SELECT CATEGORY, TITLE, ACTOR, PRICE, SPECIAL, COMMON_PROD_ID FROM PRODUCTS WHERE PROD_ID = ? ")

      // Iterate Over ResultSet
      while (rs.next) {
        val orderId = rs.getInt("ORDERID")

        selectOrderProduct.setInt(1, orderId)
        val rsOrderProduct = selectOrderProduct.executeQuery()
        var products = new StringBuilder

        while (rsOrderProduct.next()) {

          val productId = rsOrderProduct.getInt("PROD_ID")
          selectProduct.setInt(1, productId)

          val rsProducts = selectProduct.executeQuery()

          if (rsOrderProduct.getRow > 1) {
            products ++= ","
          }
          if (rsProducts.next()) {
            products ++= s"""{
              |"id": ${productId},
              |"category": "${CATEGORIES get rsProducts.getInt("CATEGORY") get}",
              |"title": "${rsProducts.getString("TITLE")}",
              |"actor": "${rsProducts.getString("ACTOR")}",
              |"price" : ${rsProducts.getDouble("PRICE")},
              |"special" : ${rsProducts.getDouble("SPECIAL")},
              |"quantity" : ${rsOrderProduct.getDouble("QUANTITY")},
              |"common_id": ${rsProducts.getDouble("COMMON_PROD_ID")}
              |}""".stripMargin
          }
        }

        val userData = s""" {"name" : "Checkout - purchase",
             | "firedOn" : "${formatDate(rs.getDate("ORDERDATE"))}",
             | "order" : {
               | "id" : "${orderId}",
               | "revenue" : ${rs.getDouble("TOTALAMOUNT")},
               | "tax" : ${rs.getDouble("TAX")} 
             | },
             | "products" : [${products.toString()}]}""".stripMargin

        exportEvent(rs.getString("CUSTOMERID"), userData)
      }
    } finally {
      conn.close
    }
  }

  def exportEvent(customerId: String, userData: String) = Future {
    try {
      Http(s"http://localhost:8080/customers/${customerId}/events").auth("2ba19f7e-a9ec-4d06-b32f-9440139f6b2c", "")
        .postData(userData).asString
        
    } catch {
      case e: Exception => {
        println(s"""Erro curl -XPOST --user 2ba19f7e-a9ec-4d06-b32f-9440139f6b2c: "http://localhost:8080/customers/${customerId}/events" -d '${userData}'""")
        e.printStackTrace()
        throw e
      }
    }
  }

  def generateUserDataFromFakeNameGenerator() {

    val connStrDs2 = "jdbc:mysql://localhost:3306/DS2?user=root&password=root"
    val connDs2 = DriverManager.getConnection(connStrDs2)
    val connStrFakeName = "jdbc:mysql://localhost:3306/fakename?user=root&password=root"
    val connFakeName = DriverManager.getConnection(connStrFakeName)
    try {

      val selectCreateDate = connDs2.prepareStatement("SELECT MIN(ORDERDATE) FROM ORDERS WHERE CUSTOMERID = ? ")

      // Configure to be Read Only
      val statement = connFakeName.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val select = connFakeName
        .prepareStatement("SELECT gender, givenname, streetaddress, city, state, zipcode, countryfull, emailaddress, "
          + "username, telephonenumber, birthday, occupation, company FROM fakenames ").executeQuery()

      while (select.next) {
        val rowIndex = select.getRow

        val userData = s""" {"gender" : "${select.getString("gender")}",
             | "name" : "${select.getString("givenname")}",
             | "address" : "${select.getString("streetaddress")}",
             | "city" : "${select.getString("city")}",
             | "state" : "${select.getString("state")}",
             | "zipcode" : "${select.getString("zipcode")}",
             | "countryfull" : "${select.getString("countryfull")}",
             | "email" : "${select.getString("emailaddress")}",
             | "username" : "${select.getString("username")}",
             | "phone" : "${select.getString("telephonenumber")}",
             | "birthday" : "${select.getString("birthday")}",
             | "occupation" : "${select.getString("occupation")}",
             | "createdAt" : "${formatDate(getCreatedAt(selectCreateDate, select.getRow()))}",
             | "company" : "${select.getString("company").replace("'", "")}"}""".stripMargin

        Future {
          Http(s"http://localhost:8080/customers/${rowIndex}").auth("2ba19f7e-a9ec-4d06-b32f-9440139f6b2c", "")
            .postData(userData).asString
  
          val custString = Http(s"http://localhost:9200/customers-triton/customer/${rowIndex}?routing=${rowIndex}").asString
          if (custString.body contains "\"found\":false") {
            println(s"Error on index ${rowIndex}: ${userData}")
          }
        }
      }
    } finally {
      connFakeName.close
      connDs2.close()
    }
  }

  def formatDate(date: Date): String = {
    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(date)
  }

  def getCreatedAt(selectCreateDate: java.sql.PreparedStatement, customerId: Int): Date = {
    selectCreateDate.setInt(1, customerId)
    val rsCreatedAt = selectCreateDate.executeQuery()

    if (rsCreatedAt.next) {
      val createdAt = rsCreatedAt.getDate(1)
      if (createdAt != null) {
        return createdAt
      }
    }
    return createRandomDate()
  }

  def createRandomDate(): Date = {
    val gc = new GregorianCalendar();
    gc.set(Calendar.YEAR, 2009);
    val dayOfYear = randBetween(1, gc.getActualMaximum(Calendar.DAY_OF_YEAR));
    gc.set(Calendar.DAY_OF_YEAR, dayOfYear);
    gc.getTime
  }

  def randBetween(start: Integer, end: Integer): Integer = {
    return start + Math.round(Math.random() * (end - start)).intValue();
  }

  val CATEGORIES = Map(1 -> "Action",
    2 -> "Animation",
    3 -> "Children",
    4 -> "Classics",
    5 -> "Comedy",
    6 -> "Documentary",
    7 -> "Drama",
    8 -> "Family",
    9 -> "Foreign",
    10 -> "Games",
    11 -> "Horror",
    12 -> "Music",
    13 -> "New",
    14 -> "Sci-Fi",
    15 -> "Sports",
    16 -> "Travel")
}