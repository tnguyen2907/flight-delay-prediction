export interface FlightFeatures {
  Quarter: number;
  Month: number;
  DayOfMonth: number;
  DayOfWeek: number;
  AirlineName: string;
  OriginAirport: string;
  OriginState: string;
  DestinationAirport: string;
  DestinationState: string;
  ScheduledDepartureTime: number;
  ScheduledArrivalTime: number;
  ScheduledElapsedTime: number;
}

export interface WeatherFeatures {
  // General weather conditions
  DepartureWeatherCondition: string;
  ArrivalWeatherCondition: string;
  DepartureExtremeWeather: boolean;
  ArrivalExtremeWeather: boolean;

  // Static weather measurements
  DepartureTemperature: number;
  DepartureDewPoint: number;
  DepartureHumidity: number;
  DepartureWindSpeed: number;
  DeparturePressure: number;
  DeparturePrecipitation: number;
  ArrivalTemperature: number;
  ArrivalDewPoint: number;
  ArrivalHumidity: number;
  ArrivalWindSpeed: number;
  ArrivalPressure: number;
  ArrivalPrecipitation: number;

  // Lag and Lead values for Departure weather
  DepartureTemperatureLag1: number;
  DepartureTemperatureLag2: number;
  DepartureTemperatureLead1: number;
  DepartureTemperatureLead2: number;
  DepartureDewPointLag1: number;
  DepartureDewPointLag2: number;
  DepartureDewPointLead1: number;
  DepartureDewPointLead2: number;
  DepartureHumidityLag1: number;
  DepartureHumidityLag2: number;
  DepartureHumidityLead1: number;
  DepartureHumidityLead2: number;
  DepartureWindSpeedLag1: number;
  DepartureWindSpeedLag2: number;
  DepartureWindSpeedLead1: number;
  DepartureWindSpeedLead2: number;
  DeparturePressureLag1: number;
  DeparturePressureLag2: number;
  DeparturePressureLead1: number;
  DeparturePressureLead2: number;
  DeparturePrecipitationLag1: number;
  DeparturePrecipitationLag2: number;
  DeparturePrecipitationLead1: number;
  DeparturePrecipitationLead2: number;

  // Lag and Lead values for Arrival weather
  ArrivalTemperatureLag1: number;
  ArrivalTemperatureLag2: number;
  ArrivalTemperatureLead1: number;
  ArrivalTemperatureLead2: number;
  ArrivalDewPointLag1: number;
  ArrivalDewPointLag2: number;
  ArrivalDewPointLead1: number;
  ArrivalDewPointLead2: number;
  ArrivalHumidityLag1: number;
  ArrivalHumidityLag2: number;
  ArrivalHumidityLead1: number;
  ArrivalHumidityLead2: number;
  ArrivalWindSpeedLag1: number;
  ArrivalWindSpeedLag2: number;
  ArrivalWindSpeedLead1: number;
  ArrivalWindSpeedLead2: number;
  ArrivalPressureLag1: number;
  ArrivalPressureLag2: number;
  ArrivalPressureLead1: number;
  ArrivalPressureLead2: number;
  ArrivalPrecipitationLag1: number;
  ArrivalPrecipitationLag2: number;
  ArrivalPrecipitationLead1: number;
  ArrivalPrecipitationLead2: number;
}

export interface Features extends FlightFeatures, WeatherFeatures {}

