import Link from 'next/link'
import React from 'react'
import Image from 'next/image'

const SideBar = () => {
  return (
    <div className="flex flex-col space-y-6 p-4">
      <Link 
        href="/" 
        className="flex items-center gap-3 py-2 px-4 text-lg font-medium rounded-lg transition-colors hover:bg-gray-200"
      >
        <Image src="/home.svg" alt="Home Icon" width={30} height={30} />
        Home
      </Link>

      <Link 
        href="/dashboard" 
        className="flex items-center gap-3 py-2 px-4 text-lg font-medium rounded-lg transition-colors hover:bg-gray-200"
      >
        <Image src="/dashboard.svg" alt="Dashboard Icon" width={30} height={30} />
        Dashboard
      </Link>

      <Link 
        href="/prediction" 
        className="flex items-center gap-3 py-2 px-4 text-lg font-medium rounded-lg transition-colors hover:bg-gray-200"
      >
        <Image src="/plane.svg" alt="Prediction Icon" width={30} height={30} />
        Prediction
      </Link>
    </div>
  )
}

export default SideBar
