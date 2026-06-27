package models

import (
	"encoding/json"
	"fmt"
	"time"
)

// FlightRecordMinimal represents the legacy/minimal record schema with only 10 fields,
// matching the schema of typical cut-down Parquet dataset files.
type FlightRecordMinimal struct {
	Year            int32    `parquet:"name=YEAR, type=INT32" json:"YEAR"`
	Month           int32    `parquet:"name=MONTH, type=INT32" json:"MONTH"`
	DayOfMonth      int32    `parquet:"name=DAY_OF_MONTH, type=INT32" json:"DAY_OF_MONTH"`
	OpUniqueCarrier string   `parquet:"name=OP_UNIQUE_CARRIER, type=BYTE_ARRAY, convertedtype=UTF8" json:"OP_UNIQUE_CARRIER"`
	CrsDepTime      int32    `parquet:"name=CRS_DEP_TIME, type=INT32" json:"CRS_DEP_TIME"`
	DepDelay        *float64 `parquet:"name=DEP_DELAY, type=DOUBLE" json:"DEP_DELAY"`
	Cancelled       *float64 `parquet:"name=CANCELLED, type=DOUBLE" json:"CANCELLED"`
	Diverted        *float64 `parquet:"name=DIVERTED, type=DOUBLE" json:"DIVERTED"`
	OriginAirportID *int32   `parquet:"name=ORIGIN_AIRPORT_ID, type=INT32" json:"ORIGIN_AIRPORT_ID"`
	DestAirportID   *int32   `parquet:"name=DEST_AIRPORT_ID, type=INT32" json:"DEST_AIRPORT_ID"`
}

// ToFull converts a FlightRecordMinimal to a full FlightRecord.
func (m *FlightRecordMinimal) ToFull() FlightRecord {
	return FlightRecord{
		Year:            m.Year,
		Month:           m.Month,
		DayOfMonth:      m.DayOfMonth,
		OpUniqueCarrier: m.OpUniqueCarrier,
		CrsDepTime:      m.CrsDepTime,
		DepDelay:        m.DepDelay,
		Cancelled:       m.Cancelled,
		Diverted:        m.Diverted,
		OriginAirportID: m.OriginAirportID,
		DestAirportID:   m.DestAirportID,
	}
}

// FlightRecord represents a full record of flight data,
// containing all fields from the original BTS dataset.
type FlightRecord struct {
	Year               int32    `parquet:"name=YEAR, type=INT32" json:"YEAR"`
	Month              int32    `parquet:"name=MONTH, type=INT32" json:"MONTH"`
	DayOfMonth         int32    `parquet:"name=DAY_OF_MONTH, type=INT32" json:"DAY_OF_MONTH"`
	OpUniqueCarrier    string   `parquet:"name=OP_UNIQUE_CARRIER, type=BYTE_ARRAY, convertedtype=UTF8" json:"OP_UNIQUE_CARRIER"`
	OpCarrierFlNum     *string  `parquet:"name=OP_CARRIER_FL_NUM, type=BYTE_ARRAY, convertedtype=UTF8" json:"OP_CARRIER_FL_NUM,omitempty"`
	OriginAirportID    *int32   `parquet:"name=ORIGIN_AIRPORT_ID, type=INT32" json:"ORIGIN_AIRPORT_ID,omitempty"`
	OriginCityMarketID *int32   `parquet:"name=ORIGIN_CITY_MARKET_ID, type=INT32" json:"ORIGIN_CITY_MARKET_ID,omitempty"`
	OriginStateAbr     *string  `parquet:"name=ORIGIN_STATE_ABR, type=BYTE_ARRAY, convertedtype=UTF8" json:"ORIGIN_STATE_ABR,omitempty"`
	DestAirportID      *int32   `parquet:"name=DEST_AIRPORT_ID, type=INT32" json:"DEST_AIRPORT_ID,omitempty"`
	DestCityMarketID   *int32   `parquet:"name=DEST_CITY_MARKET_ID, type=INT32" json:"DEST_CITY_MARKET_ID,omitempty"`
	DestStateAbr       *string  `parquet:"name=DEST_STATE_ABR, type=BYTE_ARRAY, convertedtype=UTF8" json:"DEST_STATE_ABR,omitempty"`
	CrsDepTime         int32    `parquet:"name=CRS_DEP_TIME, type=INT32" json:"CRS_DEP_TIME"`
	DepTime            *int32   `parquet:"name=DEP_TIME, type=INT32" json:"DEP_TIME,omitempty"`
	DepDelay           *float64 `parquet:"name=DEP_DELAY, type=DOUBLE" json:"DEP_DELAY,omitempty"`
	CrsArrTime         *int32   `parquet:"name=CRS_ARR_TIME, type=INT32" json:"CRS_ARR_TIME,omitempty"`
	ArrTime            *int32   `parquet:"name=ARR_TIME, type=INT32" json:"ARR_TIME,omitempty"`
	ArrDelay           *float64 `parquet:"name=ARR_DELAY, type=DOUBLE" json:"ARR_DELAY,omitempty"`
	Cancelled          *float64 `parquet:"name=CANCELLED, type=DOUBLE" json:"CANCELLED,omitempty"`
	CancellationCode   *string  `parquet:"name=CANCELLATION_CODE, type=BYTE_ARRAY, convertedtype=UTF8" json:"CANCELLATION_CODE,omitempty"`
	Diverted           *float64 `parquet:"name=DIVERTED, type=DOUBLE" json:"DIVERTED,omitempty"`
	ActualElapsedTime  *float64 `parquet:"name=ACTUAL_ELAPSED_TIME, type=DOUBLE" json:"ACTUAL_ELAPSED_TIME,omitempty"`
	Distance           *float64 `parquet:"name=DISTANCE, type=DOUBLE" json:"DISTANCE,omitempty"`
	CarrierDelay       *float64 `parquet:"name=CARRIER_DELAY, type=DOUBLE" json:"CARRIER_DELAY,omitempty"`
	WeatherDelay       *float64 `parquet:"name=WEATHER_DELAY, type=DOUBLE" json:"WEATHER_DELAY,omitempty"`
	NasDelay           *float64 `parquet:"name=NAS_DELAY, type=DOUBLE" json:"NAS_DELAY,omitempty"`
	SecurityDelay      *float64 `parquet:"name=SECURITY_DELAY, type=DOUBLE" json:"SECURITY_DELAY,omitempty"`
	LateAircraftDelay  *float64 `parquet:"name=LATE_AIRCRAFT_DELAY, type=DOUBLE" json:"LATE_AIRCRAFT_DELAY,omitempty"`
}

// ExtractTime extracts the departure time as a time.Time object.
// It returns the time and a boolean indicating whether the extraction was successful.
func (r *FlightRecord) ExtractTime() (time.Time, bool) {
	// If any of the date components are zero, we consider the timestamp invalid
	if r.Year == 0 || r.Month == 0 || r.DayOfMonth == 0 {
		return time.Time{}, false
	}

	// CRS_DEP_TIME is in the format HHMM, e.g., 1530 for 15:30
	hour := int(r.CrsDepTime) / 100
	minute := int(r.CrsDepTime) % 100

	// Create a time.Time object using the extracted date and time components.
	// We assume the time zone is UTC for simplicity.
	return time.Date(int(r.Year), time.Month(r.Month), int(r.DayOfMonth), hour, minute, 0, 0, time.UTC), true
}

// Key generates a unique key for the record.
// It uses the Event Time as the key, or falls back to the current time if is invalid.
func (r *FlightRecord) Key() string {
	flightTime, timeFound := r.ExtractTime()
	if timeFound {
		return fmt.Sprintf("%d", flightTime.UnixNano())
	}

	// Fallback to current time if the flight time is not valid
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// String returns a string representation of the record.
// It formats the fields of the record into a readable string format.
func (r *FlightRecord) String() string {
	return fmt.Sprintf("Year: %d, Month: %d, DayOfMonth: %d, Carrier: %s, CrsDepTime: %d, DepDelay: %v, Cancelled: %v, Diverted: %v, OriginAirportID: %v, DestAirportID: %v",
		r.Year, r.Month, r.DayOfMonth, r.OpUniqueCarrier, r.CrsDepTime,
		deref(r.DepDelay), deref(r.Cancelled), deref(r.Diverted),
		derefInt(r.OriginAirportID), derefInt(r.DestAirportID),
	)
}

// Json converts the record to a JSON byte slice.
// It returns the JSON representation of the record and any error that occurs during marshaling.
func (r *FlightRecord) Json() ([]byte, error) {
	return json.Marshal(r)
}

// deref is a helper function that takes a pointer to a float64 and returns its value.
// If the pointer is nil, it returns the string "null" to represent a null value in JSON.
func deref(f *float64) any {
	if f == nil {
		return "null"
	}
	return *f
}

func derefInt(i *int32) any {
	if i == nil {
		return "null"
	}
	return *i
}
