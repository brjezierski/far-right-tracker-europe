import "./globals.css";

export const metadata = {
  title: "Europe Nationalist Support Map",
  description: "Choropleth of nationalist party support across Europe",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
