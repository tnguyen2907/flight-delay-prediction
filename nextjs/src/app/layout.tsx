import type { Metadata } from "next";

import "./globals.css";
import SideBar from "./SideBar";

export const metadata: Metadata = {
  title: "Flight Prediction",
  description: "Flight Dashboard and Prediction",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>
        <aside className="fixed top-0 left-0 mt-5 h-full w-64">
          <SideBar />
        </aside>
        <main className="ml-64 mt-10">
          {children}
        </main>
      </body>
    </html>
  );
}
