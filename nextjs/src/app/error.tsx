'use client';

import React from 'react'

const ErrorPage = ({ error }: { error: Error }) => {
  console.error(error);
  return (
    <p className="text-lg">Sorry. There was an error.</p>
  )
}

export default ErrorPage
