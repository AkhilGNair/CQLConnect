package CQLConnect

import java.sql
import java.text.SimpleDateFormat
import java.util.Date

import com.datastax.driver.core.LocalDate


object DateUtils {

  val DateFormatter = new SimpleDateFormat("yyyy-MM-dd")

  // TODO: Make implicit
  def date_to_time(date: Date): sql.Timestamp = {
    new sql.Timestamp(date.getTime())
  }

  implicit def convert_cld_to_d( cld:LocalDate ) : Date = {
    new sql.Date(cld.getMillisSinceEpoch())
  }

}
