import React from 'react';
import Image from 'next/image';

const Loading = () => {
  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-white">
      <Image src="/plane-loading.gif" unoptimized width={120} height={120} alt="Loading..." />
      <p className="text-3xl mt-4">Loading...</p>
    </div>
  );
};

export default Loading;
