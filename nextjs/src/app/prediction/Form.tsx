"use client";

import React, { useState } from "react";
import Input from "./Input";
import { predictDelay } from "./predictDelay/actions";

const Form = ({ airlines }: { airlines: string[] }) => {
  const [formData, setFormData] = useState({
    departureDate: "",
    departureTime: "",
    originAirport: "",
    arrivalDate: "",
    arrivalTime: "",
    destAirport: "",
    airline: "",
  });

  const [isPredicting, setIsPredicting] = useState(false);
  const [isPredicted, setIsPredicted] = useState(false);
  const [isDelayed, setIsDelayed] = useState(false);

  const handleChange = (
    e:
      | React.ChangeEvent<HTMLInputElement>
      | React.ChangeEvent<HTMLSelectElement>
  ): void => {
    const { name, value } = e.target;
    setFormData({ ...formData, [name]: value });
  };

  const handleSubmit = async (e: React.FormEvent<HTMLFormElement>): Promise<void> => {
    e.preventDefault();

    setIsPredicting(true);

    const result = await predictDelay(formData);

    if (!result.success) {
      console.error("Failed to get prediction", result.error);
      alert(
        result.error ||
          "Sorry, something went wrong with the prediction server. Please try again later."
      );
      setIsPredicting(false);
      return;
    }

    setIsPredicted(true);
    setIsDelayed(result.prediction === 1);
    setIsPredicting(false);
  };

  return (
    <div>
      <form
        onSubmit={handleSubmit}
        className="mt-8 grid grid-cols-2 gap-x-20 w-full max-w-3xl bg-white p-6 rounded-lg shadow-md border border-gray-200"
      >
        <div className="flex flex-col">
          <Input
            label="Departure Date"
            name="departureDate"
            type="date"
            value={formData.departureDate}
            onChange={handleChange}
          />
          <Input
            label="Departure Time"
            name="departureTime"
            type="time"
            value={formData.departureTime}
            onChange={handleChange}
          />
          <Input
            label="Departure Airport"
            name="originAirport"
            type="text"
            value={formData.originAirport}
            onChange={handleChange}
            placeholder="e.g. JFK, LAX, SFO"
          />
          <label className="block font-medium text-gray-700">Airline</label>
          <select
            name="airline"
            value={formData.airline}
            onChange={handleChange}
            required
            className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
          >
            <option value="" disabled>
              Select Airline
            </option>
            {airlines.map((airline) => (
              <option key={airline} value={airline}>
                {airline}
              </option>
            ))}
          </select>
        </div>
        <div className="flex flex-col">
          <Input
            label="Arrival Date"
            name="arrivalDate"
            type="date"
            value={formData.arrivalDate}
            onChange={handleChange}
          />
          <Input
            label="Arrival Time"
            name="arrivalTime"
            type="time"
            value={formData.arrivalTime}
            onChange={handleChange}
          />
          <Input
            label="Arrival Airport"
            name="destAirport"
            type="text"
            value={formData.destAirport}
            onChange={handleChange}
            placeholder="e.g. JFK, LAX, SFO"
          />
        </div>

        <button
          type="submit"
          className="mt-10 col-span-2 bg-blue-500 text-white py-2 px-4 rounded hover:bg-blue-600 transition duration-200"
        >
          Predict Delay
        </button>
      </form>
      <div className="mt-8">
        {isPredicting ? (
          <p className="text-gray-700 text-lg font-semibold">
            <span className="animate-pulse">Predicting...</span>
          </p>
        ) : isPredicted ? ( 
          <div className="bg-white max-w-3xl p-6 rounded-lg shadow-md border border-gray-200">
            <h2 className="text-xl font-semibold mb-4">Result</h2>
            <p className="text-gray-700">
              Probability of flight delay:{" "}
              <i>
              <b className={isDelayed ? "text-red-600" : "text-green-600"}>
                {isDelayed ? "Likely" : "Unlikely"}
              </b>
              </i>
            </p>
          </div>
        ) : null}{" "}
      </div>
    </div>
  );
};

export default Form;
