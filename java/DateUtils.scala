package CQLConnect

import java.sql
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import com.datastax.driver.core.LocalDate

object DateUtils {

  var DateFormatter = new SimpleDateFormat("yyyy-MM-dd")
  DateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"))

  // TODO: Make implicit
  def date_to_time(date: Date): sql.Timestamp = {
    new sql.Timestamp(date.getTime())
  }

  implicit def convert_cld_to_d( cld:LocalDate ) : Date = {
    new sql.Date(cld.getMillisSinceEpoch())
  }

}
