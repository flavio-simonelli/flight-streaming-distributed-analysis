package models

import (
	"encoding/json"
	"fmt"
	"time"
)

// FlightRecord represents a full record of flight data,
// containing all fields from the original BTS dataset.
type FlightRecord struct {
	Year               int32    `json:"YEAR"`
	Month              int32    `json:"MONTH"`
	DayOfMonth         int32    `json:"DAY_OF_MONTH"`
	OpUniqueCarrier    string   `json:"OP_UNIQUE_CARRIER"`
	OpCarrierFlNum     *string  `json:"OP_CARRIER_FL_NUM,omitempty"`
	OriginAirportID    *int32   `json:"ORIGIN_AIRPORT_ID,omitempty"`
	OriginCityMarketID *int32   `json:"ORIGIN_CITY_MARKET_ID,omitempty"`
	OriginStateAbr     *string  `json:"ORIGIN_STATE_ABR,omitempty"`
	DestAirportID      *int32   `json:"DEST_AIRPORT_ID,omitempty"`
	DestCityMarketID   *int32   `json:"DEST_CITY_MARKET_ID,omitempty"`
	DestStateAbr       *string  `json:"DEST_STATE_ABR,omitempty"`
	CrsDepTime         int32    `json:"CRS_DEP_TIME"`
	DepTime            *int32   `json:"DEP_TIME,omitempty"`
	DepDelay           *float64 `json:"DEP_DELAY,omitempty"`
	CrsArrTime         *int32   `json:"CRS_ARR_TIME,omitempty"`
	ArrTime            *int32   `json:"ARR_TIME,omitempty"`
	ArrDelay           *float64 `json:"ARR_DELAY,omitempty"`
	Cancelled          *float64 `json:"CANCELLED,omitempty"`
	CancellationCode   *string  `json:"CANCELLATION_CODE,omitempty"`
	Diverted           *float64 `json:"DIVERTED,omitempty"`
	ActualElapsedTime  *float64 `json:"ACTUAL_ELAPSED_TIME,omitempty"`
	Distance           *float64 `json:"DISTANCE,omitempty"`
	CarrierDelay       *float64 `json:"CARRIER_DELAY,omitempty"`
	WeatherDelay       *float64 `json:"WEATHER_DELAY,omitempty"`
	NasDelay           *float64 `json:"NAS_DELAY,omitempty"`
	SecurityDelay      *float64 `json:"SECURITY_DELAY,omitempty"`
	LateAircraftDelay  *float64 `json:"LATE_AIRCRAFT_DELAY,omitempty"`
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
