import Link from 'next/link';

export default function Home() {
  return (
    <div className="max-w-4xl">
      <h1 className="title mb-6">
        Welcome to Flight Delay Prediction
      </h1>
      
      <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
        <p className="text-lg mb-4">
          This is a personal data engineering project that analyzes flight data from the U.S. Department of Transportation 
          to predict flight delays and visualize aviation trends.
        </p>
        
        <h2 className="text-xl font-semibold mt-6 mb-3">Website Components:</h2>
        <ul className="list-disc pl-6 mb-6 space-y-2">
          <li><strong>Interactive Dashboard:</strong> Explore historical flight data, analyze delay patterns, and visualize key metrics across different airlines, airports, and time periods.</li>
          <li><strong>Machine Learning Prediction Model:</strong> A Gradient Boosted Trees model trained on historical flight and weather data to predict the likelihood of delays for upcoming flights.</li>
        </ul>
        
        <p className="mb-6">
          Try out the prediction tool to check if your flight might be delayed, or explore the dashboard to discover patterns in U.S. air travel delays.
        </p>
        
        <div className="flex flex-wrap gap-4">
          <Link href="/dashboard" className="bg-gray-600 hover:bg-gray-700 text-white py-2 px-6 rounded-md transition duration-200 inline-block">
            Explore Dashboard
          </Link>
          <Link href="/prediction" className="bg-blue-500 hover:bg-blue-600 text-white py-2 px-6 rounded-md transition duration-200 inline-block">
            Try Prediction Tool
          </Link>
        </div>
      </div>
    </div>
  );
}