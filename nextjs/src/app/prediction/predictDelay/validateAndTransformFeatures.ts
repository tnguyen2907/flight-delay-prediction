import { unstable_cache } from "next/cache";
import { getStorageClient } from "../getStorageClient";

const getAirports = unstable_cache(
  async (): Promise<{
    originAirports: string[];
    destAirports: string[];
  }> =>{
    console.log("getAirports from GCS bucket");
    const storage = getStorageClient();
    const bucket = storage.bucket("flight-delay-pred-data");

    const originAirportFile = bucket.file("app/OriginAirport.txt");
    const destAirportFile = bucket.file("app/DestinationAirport.txt");

    const [originAirportData, destAirportData] = await Promise.all([
      originAirportFile.download(),
      destAirportFile.download()
    ]);

    const originAirports = originAirportData[0].toString().trim().split("\n");
    const destAirports = destAirportData[0].toString().trim().split("\n");

    return { originAirports, destAirports };
  },
  [ "airport" ],
  { revalidate: 2592000 }
);

const getAirportCoords = unstable_cache(
  async (): Promise<Record<string, [number, number]>> => {
    console.log("getAirportCoords from GCS bucket");
    const storage = getStorageClient();
    const bucket = storage.bucket("flight-delay-pred-data");

    const airportCoordsFile = bucket.file("raw/airport_coords.csv");

    const airportCoordsData = await airportCoordsFile.download();

    const airportCoords = airportCoordsData[0].toString().trim().split("\n").slice(1).reduce((acc: Record<string, [number, number]>, line) => {
      const [iata, lat, lon] = line.split(",");
      acc[iata] = [parseFloat(lat), parseFloat(lon)];
      return acc;
    }, {});

    return airportCoords;
  },
  [ "airportCoord" ],
  { revalidate: 2592000 }
);

const getAirportToState = unstable_cache(
  async (): Promise<Record<string, string>> => {
    console.log("getAirportToState from GCS bucket");
    const storage = getStorageClient();
    const bucket = storage.bucket("flight-delay-pred-data");

    const airportToStateFile = bucket.file("app/airport_to_state.json");

    const airportToStateData = await airportToStateFile.download();

    const airportToState = JSON.parse(airportToStateData[0].toString());

    return airportToState;
  },
  [ "airportToState" ],
  { revalidate: 2592000 }
);

async function getState(airport: string): Promise<string> {
  const airportToState = await getAirportToState();
  return airportToState[airport.toUpperCase()];
}

async function validateAirport(originAirport: string, departureAirport: string): Promise<boolean> {
  const { originAirports, destAirports } = await getAirports();
  if (
    !originAirports.includes(originAirport.toUpperCase()) ||
    !destAirports.includes(departureAirport.toUpperCase()) ||
    originAirport.toUpperCase() === departureAirport.toUpperCase()
  ) {
    return false;
  }
  return true;
}

async function getTimezoneAwareTimestamp(date: string, time: string, [lat, lon]: [number, number]): Promise<Date> {
  const url = `https://api.openweathermap.org/data/3.0/onecall?lat=${lat}&lon=${lon}&units=metric&exclude=current,minutely,alerts&appid=${process.env.OPENWEATHERMAP_API_KEY}`;
  const openWeatherMapResponse = await fetch(
    url,
    {next: { revalidate: 86400}}
  );

  const openWeatherMapJson = await openWeatherMapResponse.json();

  const timezoneOffset = openWeatherMapJson.timezone_offset;
  const timestamp = new Date(`${date}T${time}Z`);
  const timezoneAwareTimestamp = new Date(timestamp.getTime() - timezoneOffset * 1000);

  return timezoneAwareTimestamp;
}

function validateDateTime(originTimestamp: Date, destTimestamp: Date): boolean {
  if (
    originTimestamp > destTimestamp ||
    originTimestamp.getTime() - new Date().getTime() > 1000 * 60 * 60 * 24 * 365 // 1 year
  ) {
    return false;
  }
  return true;
}

export async function validateAndTransformFeatures(
  formData: {
    originAirport: string;
    destAirport: string;
    departureDate: string;
    departureTime: string;
    arrivalDate: string;
    arrivalTime: string;
    airline: string;
  }) : 
  Promise<[ 
    boolean, 
    boolean, 
    {
      originAirport: string;
      destAirport: string;
      originState: string;
      destState: string;
      originCoords: [number, number];
      destCoords: [number, number];
      departureTimeStamp: Date;
      arrivalTimeStamp: Date;
      airline: string;
    }
  ]> {
  const statesPromise = Promise.all([
        getState(formData.originAirport),
        getState(formData.destAirport)
      ]);
  
      const airportCoordsPromise = getAirportCoords();
  
      const airportValidPromise = validateAirport(formData.originAirport, formData.destAirport);
  
      // GCS bucket fetches
      const [
        [originState, destState],
        airportCoords,
        airportValid
      ] = await Promise.all([
        statesPromise,
        airportCoordsPromise,
        airportValidPromise
      ]);
  
      const originCoords = airportCoords[formData.originAirport.toUpperCase()];
      const destCoords = airportCoords[formData.destAirport.toUpperCase()];
  
      // OpenWeatherMap API fetch
      const [departureTimestamp, arrivalTimestamp] = await Promise.all([
        getTimezoneAwareTimestamp(formData.departureDate, formData.departureTime, originCoords),
        getTimezoneAwareTimestamp(formData.arrivalDate, formData.arrivalTime, destCoords)
      ]);

      const timestampValid = validateDateTime(departureTimestamp, arrivalTimestamp);
  

      const features = {
        originAirport: formData.originAirport.toUpperCase(),
        destAirport: formData.destAirport.toUpperCase(),
        originState,
        destState,
        originCoords,
        destCoords,
        departureTimeStamp: departureTimestamp,
        arrivalTimeStamp: arrivalTimestamp,
        airline: formData.airline,
      };

      return [airportValid, timestampValid, features];
}