import React from 'react'

const DashboardPage = () => {
  return (
    <div>
      <h1 className="title">Dashboard</h1>
      <div className="mt-5 max-w-screen-2xl h-screen border border-gray-300 rounded-lg shadow-lg overflow-hidden">
        <iframe
          src={process.env.LOOKER_DASHBOARD_URL}
          width="100%"
          height="100%"
          allowFullScreen
        ></iframe>
      </div>
    </div>
  )
}

export default DashboardPage
