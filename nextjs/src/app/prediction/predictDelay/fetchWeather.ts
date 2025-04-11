/* eslint-disable @typescript-eslint/no-explicit-any */
import weatherMapping from "./weatherMapping.json" assert { type: "json" }; // Mapping from OpenWeatherMap weather codes to Meteostat weather conditions
import extremeWeatherMapping from "./extremeWeatherMapping.json" assert { type: "json" }; // Mapping from Meteostat weather conditions to extreme weather conditions
import { WeatherFeatures } from "./mlSchema";

type ApiCallType = "CURRENT_HOURLY" | "CURRENT_DAILY" | "DAY_SUMMARY" | "TIME_MACHINE";

const getApiCallType = (dt: number): ApiCallType => {
  const hourDiff = (dt - Date.now()) / 3600 / 1000;
  if (hourDiff < 0) {
    return "TIME_MACHINE";
  } else if (hourDiff < 47) {   // Offset by 1 hour to be safe
    return "CURRENT_HOURLY";
  } else if (hourDiff < 95) {
    return "TIME_MACHINE";
  } else if (hourDiff < 191) {
    return "CURRENT_DAILY";
  } else {
    return "DAY_SUMMARY";
  }
}

const callOpenWeatherMapApi = async (lat: number, lon: number, dt: number): Promise<Record<string, number | string | boolean>> => {
  const result: Record<string, number | string | boolean> = {};

  const formatToDict = (temp: number, prcp: number, remains: Record<string, any>) => {
    result["Temperature"] = temp;
    result["Precipitation"] = prcp;
    result["DewPoint"] = remains["dew_point"];
    result["Humidity"] = remains["humidity"];
    result["WindSpeed"] = remains["wind_speed"];
    result["Pressure"] = remains["pressure"];
    result["WeatherCondition"] = weatherMapping[remains["weather"][0]["id"] as keyof typeof weatherMapping];
    result["ExtremeWeather"] = extremeWeatherMapping[result["WeatherCondition"] as keyof typeof extremeWeatherMapping];
  }

  const apiKey = process.env.OPENWEATHERMAP_API_KEY;
  
  const type = getApiCallType(dt);
  // console.log(new Date(dt), type);
  // console.log(new Date(Date.now()));

  const baseURL = "https://api.openweathermap.org/data/3.0/onecall";
  const oneCallURL = `${baseURL}?lat=${lat}&lon=${lon}&units=metric&exclude=current,minutely,alerts&appid=${apiKey}`;
  const timeMachineURL = `${baseURL}/timemachine?lat=${lat}&lon=${lon}&dt=${Math.floor(dt / 1000)}&units=metric&appid=${apiKey}`;
  const daySummaryURL = `${baseURL}/day_summary?lat=${lat}&lon=${lon}&date=${new Date(dt).toISOString().split("T")[0]}&units=metric&appid=${apiKey}`;
  
  if (type == "CURRENT_HOURLY") {
    const response = await fetch(
      oneCallURL,
      {next: { revalidate: 900}}
    );
    const responseJson = await response.json();
    const hourDt = Math.floor(dt / 1000 / 3600) * 3600 * 1000;
    // console.log("hourDt", hourDt);
    // console.log("Nearby hour", new Date(hourDt));

    let curJson: Record<string, any> = responseJson["hourly"][0];
    for (const json of responseJson["hourly"]){
      if (json['dt'] * 1000 == hourDt) {
        curJson = json;
        break;
      }
    }
    // console.log("Retrieved JSON", curJson);

    const prcp = 0;
    if (curJson["rain"] != undefined) {
      result["Precipitation"] += curJson["rain"]["1h"];
    }
    if (curJson["snow"] != undefined) {
      result["Precipitation"] += curJson["snow"]["1h"];
    }
    formatToDict(curJson["temp"], prcp, curJson);


  }  else if (type == "CURRENT_DAILY") {
    const response = await fetch(
      oneCallURL,
      {next: { revalidate: 3600}}
    );
    const responseJson = await response.json();
    // console.log("Full JSON", responseJson["daily"]);
    const dayDt = Math.floor(dt / 1000 / 3600 / 24) * 3600 * 24 * 1000;
    // console.log("dayDt", dayDt);
    // console.log("Nearby day", new Date(dayDt));

    let curJson: Record<string, any> = responseJson["daily"][4];
    let min_delta = Infinity;
    for (const json of responseJson["daily"]){
      const delta = Math.abs(json['dt'] - dayDt / 1000);
      // console.log("Delta", delta);
      if (delta < min_delta) {
        min_delta = delta;
        curJson = json;
      }
    }
    // console.log("Retrieved JSON", curJson);

    let temp = 0;
    const hour = Math.floor(dt / 1000 / 3600);
    if (hour < 6) {
      temp = curJson["temp"]["morn"];
    } else if (hour < 12) {
      temp = curJson["temp"]["day"];
    } else if (hour < 18) {
      temp = curJson["temp"]["eve"];
    } else {
      temp = curJson["temp"]["night"];
    }
    const prcp = 0;
    if (curJson["rain"] != undefined) {
      result["Precipitation"] += curJson["rain"];
    }
    if (curJson["snow"] != undefined) {
      result["Precipitation"] += curJson["snow"];
    }
    formatToDict(temp, prcp, curJson);


  } else if (type == "TIME_MACHINE") {
    const response = await fetch(
      timeMachineURL,
      {next: { revalidate: 3600}}
    );
    const responseJson = await response.json();
    const curJson = responseJson["data"][0]
    // console.log("Retrieved JSON", curJson);

    const prcp = 0;
    if (curJson["rain"] != undefined) {
      result["Precipitation"] += curJson["rain"]["1h"];
    }
    if (curJson["snow"] != undefined) {
      result["Precipitation"] += curJson["snow"]["1h"];
    }
    formatToDict(curJson["temp"], prcp, curJson);


  } else {    // DAY_SUMMARY
    // console.log(daySummaryURL);
    const response = await fetch(
      daySummaryURL,
      {next: { revalidate: 604800}}
    );
    // console.log("Response", response);
    const curJson = await response.json();
    // console.log("Retrieved JSON", curJson);

    let temp = 0;
    const hour = Math.floor(dt / 1000 / 3600);
    if (hour < 6) {
      temp = curJson["temperature"]["night"];
    } else if (hour < 12) {
      temp = curJson["temperature"]["morning"];
    } else if (hour < 18) {
      temp = curJson["temperature"]["afternoon"];
    } else {
      temp = curJson["temperature"]["evening"];
    }
    result["Temperature"] = temp;
    result["Precipitation"] = curJson["precipitation"]["total"];
    result["DewPoint"] = 0;
    result["Humidity"] = curJson["humidity"]["afternoon"];
    result["Pressure"] = curJson["pressure"]["afternoon"];
    result["WindSpeed"] = curJson["wind"]["max"]["speed"];
    result["WeatherCondition"] = result["Precipitation"] as number > 0 ? "Rain" : "Clear";
    result["ExtremeWeather"] = false;
  }
  
  return result;
}

const fetchWeather = async (
  originCoords: [number, number],
  destCoords: [number, number],
  departureTimestamp: Date,
  arrivalTimestamp: Date
): Promise<WeatherFeatures> => {
  const weatherFeatures: Record<string, number | string | boolean> = {};

  for (const prefix of ["Departure", "Arrival"]) {
    const [lat, lon] = prefix == "Departure" ? originCoords : destCoords;
    const timestamp = prefix == "Departure" ? departureTimestamp : arrivalTimestamp;

    for (const lagLead of [-2, -1, 0, 1, 2]) {
      const dt = timestamp.getTime() + lagLead * 3600 * 1000;
      const openWeatherMapResponse = await callOpenWeatherMapApi(lat, lon, dt);

      // Iterate over all weather features to add prefixes and suffixes
      for (const key of Object.keys(openWeatherMapResponse)) {
        let suffix = ""
        if (lagLead < 0) suffix = `Lag${Math.abs(lagLead)}`;
        else if (lagLead > 0) suffix = `Lead${lagLead}`;
        weatherFeatures[`${prefix}${key}${suffix}`] = openWeatherMapResponse[key];
      }
    }
  }

  return weatherFeatures as unknown as WeatherFeatures;
}

export default fetchWeather;