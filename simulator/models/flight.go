package models

import (
	"encoding/json"
	"fmt"
	"time"
)

// FlightRecord represents a flight record with BTS dataset fields.
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

// ExtractTime returns the departure time as a time.Time.
func (r *FlightRecord) ExtractTime() (time.Time, bool) {
	if r.Year == 0 || r.Month == 0 || r.DayOfMonth == 0 {
		return time.Time{}, false
	}

	hour := int(r.CrsDepTime) / 100
	minute := int(r.CrsDepTime) % 100

	return time.Date(int(r.Year), time.Month(r.Month), int(r.DayOfMonth), hour, minute, 0, 0, time.UTC), true
}

// Key generates a unique record key based on the event time.
func (r *FlightRecord) Key() string {
	flightTime, timeFound := r.ExtractTime()
	if timeFound {
		return fmt.Sprintf("%d", flightTime.UnixNano())
	}
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// String returns a readable string representation of the record.
func (r *FlightRecord) String() string {
	return fmt.Sprintf("Year: %d, Month: %d, DayOfMonth: %d, Carrier: %s, CrsDepTime: %d, DepDelay: %v, Cancelled: %v, Diverted: %v, OriginAirportID: %v, DestAirportID: %v",
		r.Year, r.Month, r.DayOfMonth, r.OpUniqueCarrier, r.CrsDepTime,
		deref(r.DepDelay), deref(r.Cancelled), deref(r.Diverted),
		derefInt(r.OriginAirportID), derefInt(r.DestAirportID),
	)
}

// Json marshals the record to JSON.
func (r *FlightRecord) Json() ([]byte, error) {
	return json.Marshal(r)
}

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
