import { unstable_cache } from "next/cache";
import Form from "./Form"
import { getStorageClient } from "./getStorageClient";

const getAirlines = unstable_cache(
  async (): Promise<string[]> => {
    console.log("getAirlines from GCS bucket");
    const storage = getStorageClient();
    const bucket = storage.bucket("flight-delay-pred-data");

    const airlineFile = bucket.file("app/AirlineName.txt");

    const airlineData = await airlineFile.download();

    const airlines = airlineData[0].toString().trim().split("\n");

    return airlines;
  },
  [ "airline" ],
  { revalidate: 2592000 }
);

const PredictionPage = async () => {
  const airlines = await getAirlines();
  console.log(process.env.GCP_PROJECT_ID);
  console.log(process.env.NODE_ENV);
  console.log(process.env.GOOGLE_APPLICATION_CREDENTIALS);
  return (
    <div>
      <h1 className="title">Flight Delay Prediction</h1>
      <p className='mt-5 text-gray-600'>Get an estimate of how likely your flight will be delayed.</p>
      <div className="mt-3 p-3 max-w-3xl bg-amber-50 border border-amber-200 rounded-md text-amber-700">
        <p className="text-sm flex items-center">
          <span><strong>Note:</strong> The first prediction after a period of inactivity may take 3-5 minutes while the prediction server scales up.</span>
        </p>
      </div>      
      <Form airlines={airlines}/>
    </div>
    
  )
}

export default PredictionPage
