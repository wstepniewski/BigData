package agh.wggios.analizadanych.transformations

import agh.wggios.analizadanych.caseclass.FlightCaseClass

class Transformations {
  def USA_flights(row: FlightCaseClass): Boolean ={
    row.DEST_COUNTRY_NAME == "United States" || row.ORIGIN_COUNTRY_NAME == "United States"
  }
}
