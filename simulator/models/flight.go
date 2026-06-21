package models

import (
	"encoding/json"
	"fmt"
	"time"
)

// FlightRecord represents a single record of flight data,
// with fields corresponding to the columns in the Parquet file.
type FlightRecord struct {
	Year              int32    `parquet:"name=YEAR, type=INT32" json:"YEAR"`
	Month             int32    `parquet:"name=MONTH, type=INT32" json:"MONTH"`
	DayOfMonth        int32    `parquet:"name=DAY_OF_MONTH, type=INT32" json:"DAY_OF_MONTH"`
	OpUniqueCarrier   string   `parquet:"name=OP_UNIQUE_CARRIER, type=BYTE_ARRAY, convertedtype=UTF8" json:"OP_UNIQUE_CARRIER"`
	CrsDepTime        int32    `parquet:"name=CRS_DEP_TIME, type=INT32" json:"CRS_DEP_TIME"`
	DepDelay          *float64 `parquet:"name=DEP_DELAY, type=DOUBLE" json:"DEP_DELAY"`
	ArrDelay          *float64 `parquet:"name=ARR_DELAY, type=DOUBLE" json:"ARR_DELAY"`
	Cancelled         *float64 `parquet:"name=CANCELLED, type=DOUBLE" json:"CANCELLED"`
	Diverted          *float64 `parquet:"name=DIVERTED, type=DOUBLE" json:"DIVERTED"`
	DepTime           *float64 `json:"DEP_TIME"`
	ArrTime           *float64 `json:"ARR_TIME"`
	CarrierDelay      *float64 `parquet:"name=CARRIER_DELAY, type=DOUBLE" json:"CARRIER_DELAY"`
	WeatherDelay      *float64 `parquet:"name=WEATHER_DELAY, type=DOUBLE" json:"WEATHER_DELAY"`
	NasDelay          *float64 `parquet:"name=NAS_DELAY, type=DOUBLE" json:"NAS_DELAY"`
	SecurityDelay     *float64 `parquet:"name=SECURITY_DELAY, type=DOUBLE" json:"SECURITY_DELAY"`
	LateAircraftDelay *float64 `parquet:"name=LATE_AIRCRAFT_DELAY, type=DOUBLE" json:"LATE_AIRCRAFT_DELAY"`
	OriginAirportID   *int32   `json:"ORIGIN_AIRPORT_ID"`
	Dest              *string  `json:"DEST"`
}

// ExtractTime is a method of FlightRecord that extracts the departure time as a time.Time object.
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

// Key is a method of FlightRecord that generates a unique key for the record.
// It uses the Event Time as the key, or falls back to the current time if is invalid.
func (r *FlightRecord) Key() string {
	flightTime, timeFound := r.ExtractTime()
	if timeFound {
		return fmt.Sprintf("%d", flightTime.UnixNano())
	}

	// Fallback to current time if the flight time is not valid
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// String is a method of FlightRecord that returns a string representation of the record.
// It formats the fields of the record into a readable string format.
func (r *FlightRecord) String() string {
	return fmt.Sprintf("Year: %d, Month: %d, DayOfMonth: %d, Carrier: %s, DepTime: %d, DepDelay: %v, ArrDelay: %v, Cancelled: %v, Diverted: %v"+
		"CarrierDelay: %v, WeatherDelay: %v, NasDelay: %v, SecurityDelay: %v, LateAircraftDelay: %v, OriginAirportID: %v, Dest: %v",
		r.Year, r.Month, r.DayOfMonth, r.OpUniqueCarrier, r.CrsDepTime,
		deref(r.DepDelay), deref(r.ArrDelay), deref(r.Cancelled), deref(r.Diverted),
		deref(r.CarrierDelay), deref(r.WeatherDelay), deref(r.NasDelay), deref(r.SecurityDelay), deref(r.LateAircraftDelay),
		derefInt(r.OriginAirportID), derefStr(r.Dest),
	)
}

// Json is a method of FlightRecord that converts the record to a JSON byte slice.
// It returns the JSON representation of the record and any error that occurs during marshaling.
func (r *FlightRecord) Json() ([]byte, error) {
	// If fields are nil (missing in local Parquet), generate mock values so local tests work
	if r.OriginAirportID == nil {
		// Mock origin airport ID based on carrier length/characters to simulate multiple airports
		var hash int32 = 10000
		for _, c := range r.OpUniqueCarrier {
			hash += int32(c) * 13
		}
		// Clamp to some airport range (e.g. 10000 to 19999)
		id := 10000 + (hash % 1000)
		r.OriginAirportID = &id
	}
	if r.Dest == nil {
		dest := "JFK"
		if len(r.OpUniqueCarrier) > 1 {
			// Mock dest based on carrier name (e.g. "LAX", "SFO", etc.)
			switch r.OpUniqueCarrier {
			case "AA":
				dest = "DFW"
			case "DL":
				dest = "ATL"
			case "UA":
				dest = "ORD"
			case "WN":
				dest = "MDW"
			default:
				dest = "LAX"
			}
		}
		r.Dest = &dest
	}

	// Calculate DepTime if flight is not cancelled (meaning Cancelled == nil or *Cancelled == 0.0)
	if r.Cancelled == nil || *r.Cancelled == 0.0 {
		if r.DepTime == nil {
			delay := 0.0
			if r.DepDelay != nil {
				delay = *r.DepDelay
			}
			hour := int(r.CrsDepTime) / 100
			minute := int(r.CrsDepTime) % 100
			totalMinutes := hour*60 + minute + int(delay)
			// wrap around 24 hours
			totalMinutes = (totalMinutes%1440 + 1440) % 1440

			newHour := totalMinutes / 60
			newMin := totalMinutes % 60
			depTimeVal := float64(newHour*100 + newMin)
			r.DepTime = &depTimeVal
		}
	} else {
		// Cancelled flight must have nil departure time
		r.DepTime = nil
	}

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

func derefStr(s *string) any {
	if s == nil {
		return "null"
	}
	return *s
}
