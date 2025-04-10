'use server'

import fetchWeather from "./fetchWeather";
import { Features, FlightFeatures, WeatherFeatures } from "./mlSchema";
import { validateAndTransformFeatures } from "./validateAndTransformFeatures";

let dailyCount = 0; 
let lastResetDay = new Date().toISOString().slice(0, 10);

export async function predictDelay(formData: {
  originAirport: string;
  destAirport: string;
  departureDate: string;
  departureTime: string;
  arrivalDate: string;
  arrivalTime: string;
  airline: string;
}) {

  // Reset daily count at midnight
  const today = new Date().toISOString().slice(0, 10);
  if (today !== lastResetDay) {
    dailyCount = 0;
    lastResetDay = today;
  }

  const maxDailyCount = 20; 
  if (dailyCount > maxDailyCount) {
    return { 
      success: false, 
      error: `Sorry, the prediction server has reached its daily limit of ${maxDailyCount} requests. Please try again tomorrow.`
    };
  }

  dailyCount++;

  let airportValid: boolean;
  let timestampValid: boolean;
  let transformedFeatures: {
    originAirport: string;
    destAirport: string;
    originState: string;
    destState: string;
    originCoords: [number, number];
    destCoords: [number, number];
    departureTimeStamp: Date;
    arrivalTimeStamp: Date;
    airline: string;
  };
  
  try {
    [airportValid, timestampValid, transformedFeatures] = await validateAndTransformFeatures(formData);
  }
  catch (e) {
    console.error(e);
    return { 
      success: false, 
      error: "Sorry, something went wrong when validating and transforming features." 
    };
  }

  if (!airportValid) {
    return { 
      success: false, 
      error: "Invalid Airport Code. Please enter the IATA code of the airport (e.g. JFK, LAX, SFO). Make sure the Origin and Destination Airport are not the same" 
    };
  }

  if (!timestampValid) {
    return { 
      success: false, 
      error: "Make sure arrival datetime is not before departure datetime and within 1 year from now" 
    };
  }

  // Calculate features
  const date = transformedFeatures.departureTimeStamp;
  const month = date.getMonth() + 1;
  const day = date.getDate();
  const quarter = Math.floor((month - 1) / 3) + 1;
  let dayOfWeek = date.getDay();
  if (dayOfWeek === 0) dayOfWeek = 7;

  const localDepartureTime = new Date(`${formData.departureDate}T${formData.departureTime}`);
  const departureTimeInt = localDepartureTime.getHours() * 100 + localDepartureTime.getMinutes();

  const localArrivalTime = new Date(`${formData.arrivalDate}T${formData.arrivalTime}`);
  const arrivalTimeInt = localArrivalTime.getHours() * 100 + localArrivalTime.getMinutes();

  const elapsedTime = (transformedFeatures.arrivalTimeStamp.getTime() - transformedFeatures.departureTimeStamp.getTime()) / 60000;
  
  // Get weather data
  let weatherFeatures: WeatherFeatures;
  try {
    weatherFeatures = await fetchWeather(transformedFeatures.originCoords, transformedFeatures.destCoords, transformedFeatures.departureTimeStamp, transformedFeatures.arrivalTimeStamp);
  } catch (e) {
    console.error(e);
    return { 
      success: false, 
      error: "Sorry, something went wrong when fetching weather data." 
    };
  }

  const flightFeatures: FlightFeatures = {
    "Quarter": quarter,
    "Month": month,
    "DayOfMonth": day,
    "DayOfWeek": dayOfWeek,
    "AirlineName": transformedFeatures.airline,
    "OriginAirport": transformedFeatures.originAirport,
    "OriginState": transformedFeatures.originState,
    "DestinationAirport": transformedFeatures.destAirport,
    "DestinationState": transformedFeatures.destState,
    "ScheduledDepartureTime": departureTimeInt,
    "ScheduledArrivalTime": arrivalTimeInt,
    "ScheduledElapsedTime": elapsedTime,
  }

  const features: Features = { ...flightFeatures, ...weatherFeatures};

  // Make prediction
  try {
    const response = await fetch(`${process.env.PREDICTION_API_URL}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${process.env.DATABRICKS_TOKEN}`
      },
      body: JSON.stringify({ dataframe_records: [features] }),
      next: { revalidate: 900 }
    });

    if (!response.ok) {
      return { 
        success: false, 
        error: "Sorry, something went wrong with the prediction server" 
      };
    }

    const responseBody = await response.json();
    const prediction = responseBody["predictions"][0];

    return { 
      success: true, 
      prediction 
    };
  } catch (error) {
    console.error(error);
    return { 
      success: false, 
      error: "Sorry, something went wrong with the prediction server." 
    };
  }
}