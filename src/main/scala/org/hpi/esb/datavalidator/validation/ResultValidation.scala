package org.hpi.esb.datavalidator.validation

import org.hpi.esb.datavalidator.consumer.Records

abstract class ResultValidation {
  def execute(records: Records): Boolean
}
